use std::collections::{HashMap, LinkedList, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::{CrioError, FrameId, PageId, Result, INVALID_PAGE_ID, PAGE_SIZE};
use crate::storage::disk::{DiskManager, DiskScheduler};

use super::{FrameHeader, LruKReplacer, ReadPageGuard, WritePageGuard};

const PREFETCH_LOOKAHEAD: u32 = 4;
const SEQUENTIAL_THRESHOLD: usize = 3;

struct AccessTracker {
    recent_accesses: VecDeque<PageId>,
}

impl AccessTracker {
    fn new() -> Self {
        Self {
            recent_accesses: VecDeque::with_capacity(SEQUENTIAL_THRESHOLD + 1),
        }
    }

    fn record(&mut self, page_id: PageId) {
        if self.recent_accesses.len() >= SEQUENTIAL_THRESHOLD + 1 {
            self.recent_accesses.pop_front();
        }
        self.recent_accesses.push_back(page_id);
    }

    fn is_sequential(&self) -> bool {
        if self.recent_accesses.len() < SEQUENTIAL_THRESHOLD {
            return false;
        }

        let accesses: Vec<_> = self.recent_accesses.iter().collect();
        for i in 1..accesses.len() {
            if accesses[i].as_u32() != accesses[i - 1].as_u32() + 1 {
                return false;
            }
        }
        true
    }
}

struct BufferPoolState {
    frames: Vec<Arc<FrameHeader>>,
    page_table: Mutex<HashMap<PageId, FrameId>>,
    free_list: Mutex<LinkedList<FrameId>>,
    replacer: LruKReplacer,
    access_tracker: Mutex<AccessTracker>,
}

/// BufferPoolManager is responsible for fetching database pages from disk
/// and storing them in memory. It manages a fixed number of frames and uses
/// the LRU-K replacement policy to decide which pages to evict.
pub struct BufferPoolManager {
    /// Number of frames in the buffer pool
    pool_size: usize,
    /// Shared state
    state: Arc<BufferPoolState>,
    /// Disk scheduler for async I/O
    disk_scheduler: DiskScheduler,
}

impl BufferPoolManager {
    /// Creates a new BufferPoolManager with the given pool size, k value for LRU-K,
    /// and disk manager.
    pub fn new(pool_size: usize, k: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = LinkedList::new();

        for i in 0..pool_size {
            let frame_id = FrameId::new(i as u32);
            frames.push(Arc::new(FrameHeader::new(frame_id)));
            free_list.push_back(frame_id);
        }

        let state = Arc::new(BufferPoolState {
            frames,
            page_table: Mutex::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            replacer: LruKReplacer::new(k, pool_size),
            access_tracker: Mutex::new(AccessTracker::new()),
        });

        Self {
            pool_size,
            state,
            disk_scheduler: DiskScheduler::new(disk_manager),
        }
    }

    /// Creates a new page in the buffer pool.
    /// Returns the page ID of the new page, or an error if no frames are available.
    /// The page is initially evictable. Use checked_write_page or checked_read_page
    /// to get a guard that pins the page.
    pub fn new_page(&self) -> Result<PageId> {
        let frame_id = self.get_free_frame()?;
        let frame = &self.state.frames[frame_id.as_usize()];

        // Allocate a new page on disk
        let page_id = self.disk_scheduler.disk_manager().allocate_page()?;

        // Initialize the frame (don't pin - let the guard handle pinning)
        frame.reset();
        frame.set_page_id(page_id);

        // Update page table
        self.state.page_table.lock().insert(page_id, frame_id);

        // Record access and mark as evictable (caller should get a guard to pin)
        self.state.replacer.record_access(frame_id);
        self.state.replacer.set_evictable(frame_id, true);

        Ok(page_id)
    }

    /// Deletes a page from the buffer pool and disk.
    /// Returns true if the page was successfully deleted.
    pub fn delete_page(&self, page_id: PageId) -> Result<bool> {
        let mut page_table = self.state.page_table.lock();

        if let Some(frame_id) = page_table.remove(&page_id) {
            let frame = &self.state.frames[frame_id.as_usize()];

            // Cannot delete a pinned page
            if frame.pin_count() > 0 {
                // Put it back in the page table
                page_table.insert(page_id, frame_id);
                return Err(CrioError::PageStillPinned(page_id));
            }

            // Reset the frame and add it to the free list
            frame.reset();
            self.state.replacer.remove(frame_id);
            self.state.free_list.lock().push_back(frame_id);

            // Deallocate the page on disk
            self.disk_scheduler
                .disk_manager()
                .deallocate_page(page_id)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Fetches a page for read access.
    /// Returns None if the page doesn't exist and cannot be created.
    pub fn checked_read_page(&self, page_id: PageId) -> Result<Option<ReadPageGuard>> {
        if page_id == INVALID_PAGE_ID {
            return Err(CrioError::InvalidPageId(page_id));
        }

        let frame_id = self.fetch_page(page_id)?;
        let frame = Arc::clone(&self.state.frames[frame_id.as_usize()]);

        // Clone state for the callback
        let state = Arc::clone(&self.state);

        let guard = unsafe {
            ReadPageGuard::new(
                page_id,
                frame,
                Box::new(move |pid, is_dirty| {
                    let pt = state.page_table.lock();
                    if let Some(&fid) = pt.get(&pid) {
                        let frm = &state.frames[fid.as_usize()];
                        if is_dirty {
                            frm.set_dirty(true);
                        }
                        if let Some(0) = frm.unpin() {
                            state.replacer.set_evictable(fid, true);
                        }
                    }
                }),
            )
        };

        Ok(Some(guard))
    }

    /// Fetches a page for write access.
    /// Returns None if the page doesn't exist and cannot be created.
    pub fn checked_write_page(&self, page_id: PageId) -> Result<Option<WritePageGuard>> {
        if page_id == INVALID_PAGE_ID {
            return Err(CrioError::InvalidPageId(page_id));
        }

        let frame_id = self.fetch_page(page_id)?;
        let frame = Arc::clone(&self.state.frames[frame_id.as_usize()]);

        // Clone state for the callback
        let state = Arc::clone(&self.state);

        let guard = unsafe {
            WritePageGuard::new(
                page_id,
                frame,
                Box::new(move |pid, is_dirty| {
                    let pt = state.page_table.lock();
                    if let Some(&fid) = pt.get(&pid) {
                        let frm = &state.frames[fid.as_usize()];
                        if is_dirty {
                            frm.set_dirty(true);
                        }
                        if let Some(0) = frm.unpin() {
                            state.replacer.set_evictable(fid, true);
                        }
                    }
                }),
            )
        };

        Ok(Some(guard))
    }

    /// Flushes a specific page to disk.
    pub fn flush_page(&self, page_id: PageId) -> Result<bool> {
        if page_id == INVALID_PAGE_ID {
            return Err(CrioError::InvalidPageId(page_id));
        }

        let page_table = self.state.page_table.lock();

        if let Some(&frame_id) = page_table.get(&page_id) {
            let frame = &self.state.frames[frame_id.as_usize()];

            let mut data = [0u8; PAGE_SIZE];
            frame.copy_to(&mut data);

            // Write to disk
            self.disk_scheduler.schedule_write_sync(page_id, &data)?;

            // Clear dirty flag
            frame.set_dirty(false);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Flushes all dirty pages to disk using sequential I/O when possible.
    /// Groups contiguous dirty pages and writes them in single I/O operations.
    pub fn flush_all_pages(&self) -> Result<()> {
        let page_table = self.state.page_table.lock();

        let mut dirty_pages: Vec<(PageId, FrameId)> = page_table
            .iter()
            .filter(|(_, &frame_id)| self.state.frames[frame_id.as_usize()].is_dirty())
            .map(|(&pid, &fid)| (pid, fid))
            .collect();

        if dirty_pages.is_empty() {
            return Ok(());
        }

        dirty_pages.sort_by_key(|(pid, _)| pid.as_u32());

        let mut i = 0;
        while i < dirty_pages.len() {
            let start_idx = i;
            let start_page = dirty_pages[start_idx].0;

            while i + 1 < dirty_pages.len()
                && dirty_pages[i + 1].0.as_u32() == dirty_pages[i].0.as_u32() + 1
            {
                i += 1;
            }

            let count = i - start_idx + 1;

            if count == 1 {
                let frame = &self.state.frames[dirty_pages[start_idx].1.as_usize()];
                let mut data = [0u8; PAGE_SIZE];
                frame.copy_to(&mut data);
                self.disk_scheduler.schedule_write_sync(start_page, &data)?;
                frame.set_dirty(false);
            } else {
                let mut bulk_data = vec![0u8; count * PAGE_SIZE];
                for j in 0..count {
                    let frame = &self.state.frames[dirty_pages[start_idx + j].1.as_usize()];
                    let offset = j * PAGE_SIZE;
                    frame.copy_to(&mut bulk_data[offset..offset + PAGE_SIZE]);
                }
                self.disk_scheduler.schedule_write_pages_sync(
                    start_page,
                    count as u32,
                    &bulk_data,
                )?;
                for j in 0..count {
                    let frame = &self.state.frames[dirty_pages[start_idx + j].1.as_usize()];
                    frame.set_dirty(false);
                }
            }

            i += 1;
        }

        Ok(())
    }

    /// Returns the pin count for a page.
    pub fn get_pin_count(&self, page_id: PageId) -> Option<u32> {
        let page_table = self.state.page_table.lock();

        page_table
            .get(&page_id)
            .map(|&frame_id| self.state.frames[frame_id.as_usize()].pin_count())
    }

    /// Returns the pool size.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns the number of free frames.
    pub fn free_frame_count(&self) -> usize {
        self.state.free_list.lock().len()
    }

    /// Prefetches multiple contiguous pages into the buffer pool using sequential I/O.
    /// This reads all pages in a SINGLE disk operation, then distributes them to frames.
    ///
    /// This is much faster than fetching pages one-by-one because:
    /// - ONE seek + ONE large read vs N seeks + N small reads
    /// - Ideal for table scans where pages are accessed sequentially
    ///
    /// Returns the number of pages successfully prefetched.
    /// Pages already in the buffer pool are skipped (not re-read).
    /// If there aren't enough free frames, prefetches as many as possible.
    pub fn prefetch_pages(&self, start_page_id: PageId, num_pages: u32) -> Result<u32> {
        if num_pages == 0 {
            return Ok(0);
        }

        // First, figure out which pages need to be fetched (not already in buffer pool)
        let mut pages_to_fetch: Vec<PageId> = Vec::new();
        {
            let page_table = self.state.page_table.lock();
            for i in 0..num_pages {
                let page_id = PageId::new(start_page_id.as_u32() + i);
                if !page_table.contains_key(&page_id) {
                    pages_to_fetch.push(page_id);
                }
            }
        }

        if pages_to_fetch.is_empty() {
            return Ok(0); // All pages already in buffer pool
        }

        // Get free frames for the pages we need to fetch
        let mut frame_ids: Vec<FrameId> = Vec::new();
        for _ in 0..pages_to_fetch.len() {
            match self.get_free_frame() {
                Ok(frame_id) => frame_ids.push(frame_id),
                Err(_) => break, // No more free frames available
            }
        }

        if frame_ids.is_empty() {
            return Ok(0); // No frames available
        }

        // Limit pages to fetch based on available frames
        let pages_to_fetch: Vec<PageId> =
            pages_to_fetch.into_iter().take(frame_ids.len()).collect();
        let actual_count = pages_to_fetch.len() as u32;

        // Find contiguous ranges and read them sequentially
        // For simplicity, we'll read the entire range from first to last page needed
        let first_page = pages_to_fetch.first().unwrap().as_u32();
        let last_page = pages_to_fetch.last().unwrap().as_u32();
        let range_size = (last_page - first_page + 1) as usize;

        // Read all pages in the range with ONE I/O operation
        let mut bulk_data = vec![0u8; range_size * PAGE_SIZE];
        self.disk_scheduler.schedule_read_pages_sync(
            PageId::new(first_page),
            range_size as u32,
            &mut bulk_data,
        )?;

        // Distribute pages to frames
        let mut page_table = self.state.page_table.lock();
        for (i, page_id) in pages_to_fetch.iter().enumerate() {
            let frame_id = frame_ids[i];
            let frame = &self.state.frames[frame_id.as_usize()];

            // Calculate offset within bulk_data for this page
            let page_offset = (page_id.as_u32() - first_page) as usize;
            let data_start = page_offset * PAGE_SIZE;
            let data_end = data_start + PAGE_SIZE;

            // Copy page data to frame
            frame.set_page_id(*page_id);
            frame.copy_from(&bulk_data[data_start..data_end]);
            frame.set_dirty(false);
            // Don't pin - prefetched pages are evictable until explicitly accessed

            // Update page table
            page_table.insert(*page_id, frame_id);

            // Record access and mark as evictable
            self.state.replacer.record_access(frame_id);
            self.state.replacer.set_evictable(frame_id, true);
        }

        Ok(actual_count)
    }

    /// Fetches a page into the buffer pool and returns its frame ID.
    /// If the page is already in the pool, returns its current frame.
    /// Otherwise, evicts a page if necessary and reads the page from disk.
    /// Automatically prefetches ahead if sequential access pattern is detected.
    fn fetch_page(&self, page_id: PageId) -> Result<FrameId> {
        {
            let page_table = self.state.page_table.lock();
            if let Some(&frame_id) = page_table.get(&page_id) {
                let frame = &self.state.frames[frame_id.as_usize()];
                frame.pin();
                self.state.replacer.record_access(frame_id);
                self.state.replacer.set_evictable(frame_id, false);
                return Ok(frame_id);
            }
        }

        let frame_id = self.get_free_frame()?;
        let frame = &self.state.frames[frame_id.as_usize()];

        let mut data = [0u8; PAGE_SIZE];
        self.disk_scheduler.schedule_read_sync(page_id, &mut data)?;

        frame.set_page_id(page_id);
        frame.copy_from(&data);
        frame.set_dirty(false);
        frame.pin();

        self.state.page_table.lock().insert(page_id, frame_id);

        self.state.replacer.record_access(frame_id);
        self.state.replacer.set_evictable(frame_id, false);

        self.maybe_prefetch(page_id);

        Ok(frame_id)
    }

    fn maybe_prefetch(&self, page_id: PageId) {
        let should_prefetch = {
            let mut tracker = self.state.access_tracker.lock();
            tracker.record(page_id);
            tracker.is_sequential()
        };

        if should_prefetch {
            let next_page = PageId::new(page_id.as_u32() + 1);
            let _ = self.prefetch_pages(next_page, PREFETCH_LOOKAHEAD);
        }
    }

    /// Gets a free frame, either from the free list or by evicting a page.
    fn get_free_frame(&self) -> Result<FrameId> {
        // Try to get from free list first
        {
            let mut free_list = self.state.free_list.lock();
            if let Some(frame_id) = free_list.pop_front() {
                return Ok(frame_id);
            }
        }

        // Need to evict a page
        if let Some(frame_id) = self.state.replacer.evict() {
            let frame = &self.state.frames[frame_id.as_usize()];
            let old_page_id = frame.page_id();

            // If the page is dirty, flush it to disk first
            if frame.is_dirty() {
                let mut data = [0u8; PAGE_SIZE];
                frame.copy_to(&mut data);
                self.disk_scheduler
                    .schedule_write_sync(old_page_id, &data)?;
            }

            // Remove from page table
            self.state.page_table.lock().remove(&old_page_id);

            // Reset the frame
            frame.reset();

            Ok(frame_id)
        } else {
            Err(CrioError::BufferPoolFull)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_bpm(pool_size: usize) -> (BufferPoolManager, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
        let bpm = BufferPoolManager::new(pool_size, 2, dm);
        (bpm, temp_file)
    }

    #[test]
    fn test_buffer_pool_manager_new() {
        let (bpm, _temp) = create_bpm(10);
        assert_eq!(bpm.pool_size(), 10);
        assert_eq!(bpm.free_frame_count(), 10);
    }

    #[test]
    fn test_buffer_pool_manager_new_page() {
        let (bpm, _temp) = create_bpm(10);

        let page_id = bpm.new_page().unwrap();
        assert_eq!(page_id, PageId::new(1)); // Page 0 is directory
        assert_eq!(bpm.get_pin_count(page_id), Some(0));
        assert_eq!(bpm.free_frame_count(), 9);
    }

    #[test]
    fn test_buffer_pool_manager_read_write() {
        let (bpm, _temp) = create_bpm(10);

        let page_id = bpm.new_page().unwrap();

        // Write to the page
        {
            let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
            guard.data_mut()[0] = 42;
            guard.data_mut()[100] = 255;
        }

        // The page should now be unpinned
        assert_eq!(bpm.get_pin_count(page_id), Some(0));

        // Read back
        {
            let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
            assert_eq!(guard.data()[0], 42);
            assert_eq!(guard.data()[100], 255);
        }
    }

    #[test]
    fn test_buffer_pool_manager_flush() {
        let (bpm, temp) = create_bpm(10);

        let page_id = bpm.new_page().unwrap();

        // Write to the page
        {
            let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
            guard.data_mut()[0] = 42;
        }

        // Flush the page
        bpm.flush_page(page_id).unwrap();

        // Verify data persisted by reading from a new BPM
        drop(bpm);

        let dm = Arc::new(DiskManager::new(temp.path()).unwrap());
        let bpm2 = BufferPoolManager::new(10, 2, dm);

        let guard = bpm2.checked_read_page(page_id).unwrap().unwrap();
        assert_eq!(guard.data()[0], 42);
    }

    #[test]
    fn test_buffer_pool_manager_eviction() {
        let (bpm, _temp) = create_bpm(3);

        // Create pages and fill the buffer pool
        let page_ids: Vec<_> = (0..3).map(|_| bpm.new_page().unwrap()).collect();

        // Unpin all pages by dropping their guards
        for &pid in &page_ids {
            {
                let mut guard = bpm.checked_write_page(pid).unwrap().unwrap();
                guard.data_mut()[0] = pid.as_u32() as u8;
            }
        }

        // All pages should be evictable now
        assert_eq!(bpm.free_frame_count(), 0);

        // Create a new page - should evict one of the existing pages
        let new_page_id = bpm.new_page().unwrap();
        assert_eq!(new_page_id, PageId::new(4)); // 1,2,3 + new = 4
    }

    #[test]
    fn test_buffer_pool_manager_delete_page() {
        let (bpm, _temp) = create_bpm(10);

        let page_id = bpm.new_page().unwrap();

        // Cannot delete while pinned
        {
            let _guard = bpm.checked_read_page(page_id).unwrap().unwrap();
            assert!(bpm.delete_page(page_id).is_err());
        }

        // Can delete after unpinning
        assert!(bpm.delete_page(page_id).unwrap());
        assert_eq!(bpm.get_pin_count(page_id), None);
    }

    #[test]
    fn test_buffer_pool_manager_buffer_pool_full() {
        let (bpm, _temp) = create_bpm(2);

        // Create and keep pinned two pages
        let page_id1 = bpm.new_page().unwrap();
        let page_id2 = bpm.new_page().unwrap();

        // Keep both pages pinned
        let _guard1 = bpm.checked_read_page(page_id1).unwrap().unwrap();
        let _guard2 = bpm.checked_read_page(page_id2).unwrap().unwrap();

        // Try to create a third page - should fail
        assert!(matches!(bpm.new_page(), Err(CrioError::BufferPoolFull)));
    }

    #[test]
    fn test_buffer_pool_manager_prefetch() {
        let (bpm, _temp) = create_bpm(10);

        // Create 4 pages with data (pages 1-4, since 0 is directory)
        for i in 0..4u8 {
            let page_id = bpm.new_page().unwrap();
            let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
            guard.data_mut()[0] = i + 1;
        }

        bpm.flush_all_pages().unwrap();

        drop(bpm);
        let dm = Arc::new(DiskManager::new(_temp.path()).unwrap());
        let bpm2 = BufferPoolManager::new(10, 2, dm);

        assert_eq!(bpm2.free_frame_count(), 10);

        // Prefetch pages 1-4 (page 0 is directory)
        let prefetched = bpm2.prefetch_pages(PageId::new(1), 4).unwrap();
        assert_eq!(prefetched, 4);

        assert_eq!(bpm2.free_frame_count(), 6);

        for i in 0..4u8 {
            let guard = bpm2
                .checked_read_page(PageId::new(i as u32 + 1))
                .unwrap()
                .unwrap();
            assert_eq!(guard.data()[0], i + 1);
        }
    }

    #[test]
    fn test_prefetch_skips_cached_pages() {
        let (bpm, _temp) = create_bpm(10);

        // Create 3 pages (pages 1, 2, 3)
        for _ in 0..3 {
            bpm.new_page().unwrap();
        }

        // Prefetch pages 1-3 should skip since they're already cached
        let prefetched = bpm.prefetch_pages(PageId::new(1), 3).unwrap();

        assert_eq!(prefetched, 0);
    }
}
