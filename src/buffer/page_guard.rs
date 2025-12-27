use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::common::{PageId, PAGE_SIZE};

use super::FrameHeader;

/// Callback type for releasing a page guard
type ReleaseCallback = Box<dyn FnOnce(PageId, bool) + Send + Sync>;

/// Base page guard that manages the common functionality
struct PageGuardBase {
    /// The page ID being guarded
    page_id: PageId,
    /// Reference to the frame header (kept alive for the guard's lifetime)
    _frame: Arc<FrameHeader>,
    /// Callback to release the guard
    release_callback: Option<ReleaseCallback>,
    /// Whether the page was marked dirty
    is_dirty: bool,
}

impl PageGuardBase {
    fn new(page_id: PageId, frame: Arc<FrameHeader>, release_callback: ReleaseCallback) -> Self {
        Self {
            page_id,
            _frame: frame,
            release_callback: Some(release_callback),
            is_dirty: false,
        }
    }

    fn drop_impl(&mut self) {
        if let Some(callback) = self.release_callback.take() {
            callback(self.page_id, self.is_dirty);
        }
    }
}

/// RAII guard for read-only access to a page.
/// Automatically unpins the page when dropped.
pub struct ReadPageGuard {
    base: PageGuardBase,
    /// Read lock on the page data
    _data_guard: RwLockReadGuard<'static, Box<[u8; PAGE_SIZE]>>,
}

impl ReadPageGuard {
    /// Creates a new ReadPageGuard.
    /// # Safety
    /// The caller must ensure that the frame outlives this guard.
    pub(crate) unsafe fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        release_callback: ReleaseCallback,
    ) -> Self {
        // Acquire the read lock
        let data_guard = frame.data.read();
        // Transmute to static lifetime - the frame is kept alive via Arc
        let data_guard: RwLockReadGuard<'static, Box<[u8; PAGE_SIZE]>> =
            std::mem::transmute(data_guard);

        Self {
            base: PageGuardBase::new(page_id, frame, release_callback),
            _data_guard: data_guard,
        }
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.base.page_id
    }

    /// Returns a reference to the page data.
    pub fn data(&self) -> &[u8] {
        &self._data_guard[..]
    }

    /// Drops this guard, releasing the page.
    pub fn drop_guard(self) {
        drop(self);
    }
}

impl Deref for ReadPageGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        self.base.drop_impl();
    }
}

/// RAII guard for read-write access to a page.
/// Automatically marks the page as dirty and unpins it when dropped.
pub struct WritePageGuard {
    base: PageGuardBase,
    /// Write lock on the page data
    data_guard: Option<RwLockWriteGuard<'static, Box<[u8; PAGE_SIZE]>>>,
}

impl WritePageGuard {
    /// Creates a new WritePageGuard.
    /// # Safety
    /// The caller must ensure that the frame outlives this guard.
    pub(crate) unsafe fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        release_callback: ReleaseCallback,
    ) -> Self {
        // Acquire the write lock
        let data_guard = frame.data.write();
        // Transmute to static lifetime - the frame is kept alive via Arc
        let data_guard: RwLockWriteGuard<'static, Box<[u8; PAGE_SIZE]>> =
            std::mem::transmute(data_guard);

        Self {
            base: PageGuardBase::new(page_id, frame, release_callback),
            data_guard: Some(data_guard),
        }
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.base.page_id
    }

    /// Returns a reference to the page data.
    pub fn data(&self) -> &[u8] {
        &self.data_guard.as_ref().unwrap()[..]
    }

    /// Returns a mutable reference to the page data.
    /// Automatically marks the page as dirty.
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.base.is_dirty = true;
        &mut self.data_guard.as_mut().unwrap()[..]
    }

    /// Drops this guard, releasing the page.
    pub fn drop_guard(self) {
        drop(self);
    }
}

impl Deref for WritePageGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for WritePageGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        // Drop the data guard first to release the lock
        self.data_guard.take();
        // Then call the release callback
        self.base.drop_impl();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::FrameId;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_read_page_guard() {
        let frame = Arc::new(FrameHeader::new(FrameId::new(0)));
        frame.set_page_id(PageId::new(1));

        let mut data = [0u8; PAGE_SIZE];
        data[0] = 42;
        frame.copy_from(&data);

        let released = Arc::new(AtomicBool::new(false));
        let released_clone = released.clone();

        let guard = unsafe {
            ReadPageGuard::new(
                PageId::new(1),
                frame.clone(),
                Box::new(move |_, _| {
                    released_clone.store(true, Ordering::SeqCst);
                }),
            )
        };

        assert_eq!(guard.page_id(), PageId::new(1));
        assert_eq!(guard.data()[0], 42);
        assert!(!released.load(Ordering::SeqCst));

        drop(guard);
        assert!(released.load(Ordering::SeqCst));
    }

    #[test]
    fn test_write_page_guard() {
        let frame = Arc::new(FrameHeader::new(FrameId::new(0)));
        frame.set_page_id(PageId::new(1));

        let released = Arc::new(AtomicBool::new(false));
        let dirty = Arc::new(AtomicBool::new(false));
        let released_clone = released.clone();
        let dirty_clone = dirty.clone();

        let mut guard = unsafe {
            WritePageGuard::new(
                PageId::new(1),
                frame.clone(),
                Box::new(move |_, is_dirty| {
                    released_clone.store(true, Ordering::SeqCst);
                    dirty_clone.store(is_dirty, Ordering::SeqCst);
                }),
            )
        };

        assert_eq!(guard.page_id(), PageId::new(1));

        // Write some data
        guard.data_mut()[0] = 42;

        assert!(!released.load(Ordering::SeqCst));

        drop(guard);
        assert!(released.load(Ordering::SeqCst));
        assert!(dirty.load(Ordering::SeqCst));

        // Verify data was written
        let mut read_data = [0u8; PAGE_SIZE];
        frame.copy_to(&mut read_data);
        assert_eq!(read_data[0], 42);
    }
}
