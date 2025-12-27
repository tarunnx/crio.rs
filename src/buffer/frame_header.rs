use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::RwLock;

use crate::common::{FrameId, PageId, INVALID_PAGE_ID, PAGE_SIZE};

/// FrameHeader manages a single buffer frame in the buffer pool.
/// It stores metadata about the frame and the actual page data.
pub struct FrameHeader {
    /// The frame ID (index in the buffer pool)
    frame_id: FrameId,
    /// The page ID stored in this frame (INVALID_PAGE_ID if empty)
    page_id: RwLock<PageId>,
    /// Pin count - number of threads currently accessing this frame
    pin_count: AtomicU32,
    /// Whether the page has been modified since being read from disk
    is_dirty: AtomicBool,
    /// The actual page data (pub(crate) for page guard access)
    pub(crate) data: RwLock<Box<[u8; PAGE_SIZE]>>,
}

impl FrameHeader {
    /// Creates a new FrameHeader for the given frame ID.
    pub fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            page_id: RwLock::new(INVALID_PAGE_ID),
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
            data: RwLock::new(Box::new([0u8; PAGE_SIZE])),
        }
    }

    /// Returns the frame ID.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Returns the page ID stored in this frame.
    pub fn page_id(&self) -> PageId {
        *self.page_id.read()
    }

    /// Sets the page ID stored in this frame.
    pub fn set_page_id(&self, page_id: PageId) {
        *self.page_id.write() = page_id;
    }

    /// Returns the current pin count.
    pub fn pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Increments the pin count and returns the new value.
    pub fn pin(&self) -> u32 {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrements the pin count and returns the new value.
    /// Returns None if the pin count was already 0.
    pub fn unpin(&self) -> Option<u32> {
        loop {
            let current = self.pin_count.load(Ordering::Acquire);
            if current == 0 {
                return None;
            }
            if self
                .pin_count
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(current - 1);
            }
        }
    }

    /// Returns whether the page is dirty.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Acquire)
    }

    /// Sets the dirty flag.
    pub fn set_dirty(&self, dirty: bool) {
        self.is_dirty.store(dirty, Ordering::Release);
    }

    /// Returns a read guard to the page data.
    pub fn read_data(&self) -> parking_lot::RwLockReadGuard<'_, Box<[u8; PAGE_SIZE]>> {
        self.data.read()
    }

    /// Returns a write guard to the page data.
    pub fn write_data(&self) -> parking_lot::RwLockWriteGuard<'_, Box<[u8; PAGE_SIZE]>> {
        self.data.write()
    }

    /// Copies data from the given slice into the frame.
    pub fn copy_from(&self, data: &[u8]) {
        assert_eq!(data.len(), PAGE_SIZE);
        let mut guard = self.data.write();
        guard.copy_from_slice(data);
    }

    /// Copies data from the frame into the given slice.
    pub fn copy_to(&self, data: &mut [u8]) {
        assert_eq!(data.len(), PAGE_SIZE);
        let guard = self.data.read();
        data.copy_from_slice(&**guard);
    }

    /// Resets the frame to its initial state.
    pub fn reset(&self) {
        *self.page_id.write() = INVALID_PAGE_ID;
        self.pin_count.store(0, Ordering::Release);
        self.is_dirty.store(false, Ordering::Release);
        self.data.write().fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_header_new() {
        let frame = FrameHeader::new(FrameId::new(0));
        assert_eq!(frame.frame_id(), FrameId::new(0));
        assert_eq!(frame.page_id(), INVALID_PAGE_ID);
        assert_eq!(frame.pin_count(), 0);
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_frame_header_pin_unpin() {
        let frame = FrameHeader::new(FrameId::new(0));

        assert_eq!(frame.pin(), 1);
        assert_eq!(frame.pin(), 2);
        assert_eq!(frame.pin_count(), 2);

        assert_eq!(frame.unpin(), Some(1));
        assert_eq!(frame.unpin(), Some(0));
        assert_eq!(frame.unpin(), None);
    }

    #[test]
    fn test_frame_header_dirty() {
        let frame = FrameHeader::new(FrameId::new(0));

        assert!(!frame.is_dirty());
        frame.set_dirty(true);
        assert!(frame.is_dirty());
        frame.set_dirty(false);
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_frame_header_data() {
        let frame = FrameHeader::new(FrameId::new(0));

        let mut data = [0u8; PAGE_SIZE];
        data[0] = 42;
        data[100] = 255;
        frame.copy_from(&data);

        let mut read_data = [0u8; PAGE_SIZE];
        frame.copy_to(&mut read_data);

        assert_eq!(read_data[0], 42);
        assert_eq!(read_data[100], 255);
    }

    #[test]
    fn test_frame_header_reset() {
        let frame = FrameHeader::new(FrameId::new(0));

        frame.set_page_id(PageId::new(5));
        frame.pin();
        frame.set_dirty(true);
        let mut data = [1u8; PAGE_SIZE];
        frame.copy_from(&data);

        frame.reset();

        assert_eq!(frame.page_id(), INVALID_PAGE_ID);
        assert_eq!(frame.pin_count(), 0);
        assert!(!frame.is_dirty());

        frame.copy_to(&mut data);
        assert_eq!(data[0], 0);
    }
}
