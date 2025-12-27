use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::common::{PageId, Result};

pub const EXTENT_SIZE: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentId(u32);

impl ExtentId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn start_page(&self) -> PageId {
        PageId::new(self.0 * EXTENT_SIZE)
    }
}

#[derive(Debug, Clone)]
struct ExtentInfo {
    allocated_bitmap: u8,
    allocated_count: u8,
}

impl ExtentInfo {
    fn new() -> Self {
        Self {
            allocated_bitmap: 0,
            allocated_count: 0,
        }
    }

    fn allocate_next(&mut self) -> Option<u8> {
        if self.allocated_count >= EXTENT_SIZE as u8 {
            return None;
        }

        for i in 0..EXTENT_SIZE as u8 {
            let mask = 1 << i;
            if (self.allocated_bitmap & mask) == 0 {
                self.allocated_bitmap |= mask;
                self.allocated_count += 1;
                return Some(i);
            }
        }

        None
    }

    fn deallocate(&mut self, offset: u8) -> bool {
        let mask = 1 << offset;
        if (self.allocated_bitmap & mask) != 0 {
            self.allocated_bitmap &= !mask;
            self.allocated_count -= 1;
            true
        } else {
            false
        }
    }

    fn is_full(&self) -> bool {
        self.allocated_count == EXTENT_SIZE as u8
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.allocated_count == 0
    }
}

pub struct ExtentAllocator {
    table_extents: Mutex<HashMap<u32, Vec<ExtentId>>>,
    extent_info: Mutex<HashMap<ExtentId, ExtentInfo>>,
    next_extent_id: AtomicU32,
}

impl ExtentAllocator {
    pub fn new() -> Self {
        Self {
            table_extents: Mutex::new(HashMap::new()),
            extent_info: Mutex::new(HashMap::new()),
            next_extent_id: AtomicU32::new(0),
        }
    }

    pub fn from_existing(num_pages: u32) -> Self {
        let num_extents = (num_pages + EXTENT_SIZE - 1) / EXTENT_SIZE;

        let mut extent_info_map = HashMap::new();

        for extent_idx in 0..num_extents {
            let extent_id = ExtentId::new(extent_idx);
            let start = extent_idx * EXTENT_SIZE;
            let end = ((extent_idx + 1) * EXTENT_SIZE).min(num_pages);
            let pages_in_extent = (end - start) as u8;

            let mut info = ExtentInfo::new();
            for i in 0..pages_in_extent {
                info.allocated_bitmap |= 1 << i;
            }
            info.allocated_count = pages_in_extent;

            extent_info_map.insert(extent_id, info);
        }

        Self {
            table_extents: Mutex::new(HashMap::new()),
            extent_info: Mutex::new(extent_info_map),
            next_extent_id: AtomicU32::new(num_extents),
        }
    }

    pub fn allocate_page_for_table(&self, table_id: u32) -> Result<PageId> {
        let mut table_extents = self.table_extents.lock();
        let mut extent_info = self.extent_info.lock();

        let extents = table_extents.entry(table_id).or_insert_with(Vec::new);

        for &extent_id in extents.iter().rev() {
            if let Some(info) = extent_info.get_mut(&extent_id) {
                if !info.is_full() {
                    if let Some(offset) = info.allocate_next() {
                        let page_id = PageId::new(extent_id.as_u32() * EXTENT_SIZE + offset as u32);
                        return Ok(page_id);
                    }
                }
            }
        }

        let extent_id = ExtentId::new(self.next_extent_id.fetch_add(1, Ordering::SeqCst));
        let mut info = ExtentInfo::new();
        let offset = info.allocate_next().unwrap();

        extent_info.insert(extent_id, info);
        extents.push(extent_id);

        let page_id = PageId::new(extent_id.as_u32() * EXTENT_SIZE + offset as u32);
        Ok(page_id)
    }

    pub fn allocate_extent_for_table(&self, table_id: u32) -> Result<Vec<PageId>> {
        let mut table_extents = self.table_extents.lock();
        let mut extent_info = self.extent_info.lock();

        let extent_id = ExtentId::new(self.next_extent_id.fetch_add(1, Ordering::SeqCst));
        let mut info = ExtentInfo::new();

        let mut pages = Vec::with_capacity(EXTENT_SIZE as usize);
        for _ in 0..EXTENT_SIZE {
            if let Some(offset) = info.allocate_next() {
                pages.push(PageId::new(
                    extent_id.as_u32() * EXTENT_SIZE + offset as u32,
                ));
            }
        }

        extent_info.insert(extent_id, info);
        table_extents
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(extent_id);

        Ok(pages)
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        let extent_idx = page_id.as_u32() / EXTENT_SIZE;
        let offset = (page_id.as_u32() % EXTENT_SIZE) as u8;
        let extent_id = ExtentId::new(extent_idx);

        let mut extent_info = self.extent_info.lock();
        if let Some(info) = extent_info.get_mut(&extent_id) {
            info.deallocate(offset);
        }
    }

    pub fn get_table_extents(&self, table_id: u32) -> Vec<ExtentId> {
        self.table_extents
            .lock()
            .get(&table_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_contiguous_pages(&self, table_id: u32) -> Vec<(PageId, u32)> {
        let table_extents = self.table_extents.lock();
        let extent_info = self.extent_info.lock();

        let mut ranges = Vec::new();

        if let Some(extents) = table_extents.get(&table_id) {
            for &extent_id in extents {
                if let Some(info) = extent_info.get(&extent_id) {
                    if info.allocated_count > 0 {
                        let start_page = extent_id.start_page();
                        ranges.push((start_page, info.allocated_count as u32));
                    }
                }
            }
        }

        ranges
    }

    pub fn total_pages_allocated(&self) -> u32 {
        self.next_extent_id.load(Ordering::Relaxed) * EXTENT_SIZE
    }
}

impl Default for ExtentAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_pages_for_same_table_are_contiguous() {
        let allocator = ExtentAllocator::new();

        let p1 = allocator.allocate_page_for_table(1).unwrap();
        let p2 = allocator.allocate_page_for_table(1).unwrap();
        let p3 = allocator.allocate_page_for_table(1).unwrap();

        assert_eq!(p1, PageId::new(0));
        assert_eq!(p2, PageId::new(1));
        assert_eq!(p3, PageId::new(2));
    }

    #[test]
    fn test_different_tables_get_different_extents() {
        let allocator = ExtentAllocator::new();

        let t1_p1 = allocator.allocate_page_for_table(1).unwrap();
        let t2_p1 = allocator.allocate_page_for_table(2).unwrap();
        let t1_p2 = allocator.allocate_page_for_table(1).unwrap();
        let t2_p2 = allocator.allocate_page_for_table(2).unwrap();

        assert_eq!(t1_p1, PageId::new(0));
        assert_eq!(t1_p2, PageId::new(1));

        assert_eq!(t2_p1, PageId::new(8));
        assert_eq!(t2_p2, PageId::new(9));
    }

    #[test]
    fn test_allocate_full_extent() {
        let allocator = ExtentAllocator::new();

        let pages = allocator.allocate_extent_for_table(1).unwrap();

        assert_eq!(pages.len(), EXTENT_SIZE as usize);
        for (i, page) in pages.iter().enumerate() {
            assert_eq!(page.as_u32(), i as u32);
        }
    }

    #[test]
    fn test_extent_overflow_creates_new_extent() {
        let allocator = ExtentAllocator::new();

        for _ in 0..EXTENT_SIZE {
            allocator.allocate_page_for_table(1).unwrap();
        }

        let overflow_page = allocator.allocate_page_for_table(1).unwrap();
        assert_eq!(overflow_page, PageId::new(EXTENT_SIZE));
    }

    #[test]
    fn test_get_contiguous_pages() {
        let allocator = ExtentAllocator::new();

        for _ in 0..5 {
            allocator.allocate_page_for_table(1).unwrap();
        }

        let ranges = allocator.get_contiguous_pages(1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (PageId::new(0), 5));
    }
}
