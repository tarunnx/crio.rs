use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::common::{CrioError, PageId, Result, PAGE_SIZE};

use super::DiskManager;

/// Represents a disk I/O request
pub struct DiskRequest {
    /// Whether this is a write (true) or read (false) request
    pub is_write: bool,
    /// The starting page ID to read/write
    pub page_id: PageId,
    /// Number of pages to read/write (1 for single page, >1 for sequential I/O)
    pub num_pages: u32,
    /// Pointer to the data buffer (must be PAGE_SIZE * num_pages bytes)
    /// For reads: data will be written here
    /// For writes: data will be read from here
    pub data: *mut u8,
    /// Promise to signal completion
    pub callback: Option<std::sync::mpsc::Sender<bool>>,
}

// Safety: DiskRequest is only used by the disk scheduler thread
// and the caller must ensure the data pointer remains valid
unsafe impl Send for DiskRequest {}

impl DiskRequest {
    /// Creates a new single-page read request
    pub fn read(page_id: PageId, data: *mut u8) -> Self {
        Self {
            is_write: false,
            page_id,
            num_pages: 1,
            data,
            callback: None,
        }
    }

    /// Creates a new single-page write request
    pub fn write(page_id: PageId, data: *mut u8) -> Self {
        Self {
            is_write: true,
            page_id,
            num_pages: 1,
            data,
            callback: None,
        }
    }

    /// Creates a new sequential multi-page read request
    /// Reads num_pages starting from page_id in a single I/O operation
    pub fn read_sequential(page_id: PageId, num_pages: u32, data: *mut u8) -> Self {
        Self {
            is_write: false,
            page_id,
            num_pages,
            data,
            callback: None,
        }
    }

    /// Creates a new sequential multi-page write request
    /// Writes num_pages starting from page_id in a single I/O operation
    pub fn write_sequential(page_id: PageId, num_pages: u32, data: *mut u8) -> Self {
        Self {
            is_write: true,
            page_id,
            num_pages,
            data,
            callback: None,
        }
    }

    /// Sets the callback for this request
    pub fn with_callback(mut self, callback: std::sync::mpsc::Sender<bool>) -> Self {
        self.callback = Some(callback);
        self
    }
}

/// DiskScheduler manages a background worker thread that processes disk I/O requests.
/// It provides asynchronous disk access through a request queue.
pub struct DiskScheduler {
    /// The disk manager for actual I/O operations
    disk_manager: Arc<DiskManager>,
    /// Channel sender for queuing requests
    request_sender: Sender<DiskRequest>,
    /// Flag to signal shutdown
    shutdown: Arc<AtomicBool>,
    /// Handle to the background worker thread
    worker_handle: Option<JoinHandle<()>>,
}

impl DiskScheduler {
    /// Creates a new DiskScheduler with the given DiskManager.
    /// Spawns a background worker thread to process requests.
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let (sender, receiver) = bounded::<DiskRequest>(128);
        let shutdown = Arc::new(AtomicBool::new(false));

        let dm_clone = Arc::clone(&disk_manager);
        let shutdown_clone = Arc::clone(&shutdown);

        let worker_handle = thread::spawn(move || {
            Self::start_worker_thread(dm_clone, receiver, shutdown_clone);
        });

        Self {
            disk_manager,
            request_sender: sender,
            shutdown,
            worker_handle: Some(worker_handle),
        }
    }

    /// Schedules a disk request for processing by the background worker.
    pub fn schedule(&self, request: DiskRequest) -> Result<()> {
        self.request_sender
            .send(request)
            .map_err(|e| CrioError::DiskScheduler(format!("Failed to schedule request: {}", e)))
    }

    /// Schedules a read request and waits for completion.
    pub fn schedule_read_sync(&self, page_id: PageId, data: &mut [u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE);

        let (tx, rx) = std::sync::mpsc::channel();
        let request = DiskRequest::read(page_id, data.as_mut_ptr()).with_callback(tx);

        self.schedule(request)?;

        rx.recv().map_err(|e| {
            CrioError::DiskScheduler(format!("Failed to receive completion: {}", e))
        })?;

        Ok(())
    }

    /// Schedules a write request and waits for completion.
    pub fn schedule_write_sync(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE);

        let (tx, rx) = std::sync::mpsc::channel();
        // Safety: We're passing a const pointer but treating it as mutable in the struct
        // The worker will only read from it for writes
        let request = DiskRequest::write(page_id, data.as_ptr() as *mut u8).with_callback(tx);

        self.schedule(request)?;

        rx.recv().map_err(|e| {
            CrioError::DiskScheduler(format!("Failed to receive completion: {}", e))
        })?;

        Ok(())
    }

    /// Schedules a sequential multi-page read request and waits for completion.
    /// Reads num_pages starting from start_page_id in a SINGLE I/O operation.
    /// This is much faster than calling schedule_read_sync() multiple times.
    pub fn schedule_read_pages_sync(
        &self,
        start_page_id: PageId,
        num_pages: u32,
        data: &mut [u8],
    ) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(data.len(), expected_size);

        let (tx, rx) = std::sync::mpsc::channel();
        let request = DiskRequest::read_sequential(start_page_id, num_pages, data.as_mut_ptr())
            .with_callback(tx);

        self.schedule(request)?;

        rx.recv().map_err(|e| {
            CrioError::DiskScheduler(format!("Failed to receive completion: {}", e))
        })?;

        Ok(())
    }

    /// Schedules a sequential multi-page write request and waits for completion.
    /// Writes num_pages starting from start_page_id in a SINGLE I/O operation.
    /// This is much faster than calling schedule_write_sync() multiple times.
    pub fn schedule_write_pages_sync(
        &self,
        start_page_id: PageId,
        num_pages: u32,
        data: &[u8],
    ) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(data.len(), expected_size);

        let (tx, rx) = std::sync::mpsc::channel();
        let request =
            DiskRequest::write_sequential(start_page_id, num_pages, data.as_ptr() as *mut u8)
                .with_callback(tx);

        self.schedule(request)?;

        rx.recv().map_err(|e| {
            CrioError::DiskScheduler(format!("Failed to receive completion: {}", e))
        })?;

        Ok(())
    }

    /// The background worker thread function.
    /// Processes requests from the queue until shutdown is signaled.
    fn start_worker_thread(
        disk_manager: Arc<DiskManager>,
        receiver: Receiver<DiskRequest>,
        shutdown: Arc<AtomicBool>,
    ) {
        loop {
            // Check for shutdown
            if shutdown.load(Ordering::Relaxed) {
                // Drain remaining requests before exiting
                while let Ok(request) = receiver.try_recv() {
                    Self::process_request(&disk_manager, request);
                }
                break;
            }

            // Wait for a request with timeout
            match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(request) => {
                    Self::process_request(&disk_manager, request);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Continue loop, check shutdown flag
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    // Channel closed, exit
                    break;
                }
            }
        }
    }

    /// Processes a single disk request (supports both single-page and sequential I/O).
    fn process_request(disk_manager: &DiskManager, request: DiskRequest) {
        let total_size = (request.num_pages as usize) * PAGE_SIZE;

        let success = if request.num_pages == 1 {
            // Single page I/O (original behavior)
            if request.is_write {
                let data = unsafe { std::slice::from_raw_parts(request.data, PAGE_SIZE) };
                disk_manager.write_page(request.page_id, data).is_ok()
            } else {
                let data = unsafe { std::slice::from_raw_parts_mut(request.data, PAGE_SIZE) };
                disk_manager.read_page(request.page_id, data).is_ok()
            }
        } else {
            // Sequential multi-page I/O
            if request.is_write {
                let data = unsafe { std::slice::from_raw_parts(request.data, total_size) };
                disk_manager
                    .write_pages(request.page_id, request.num_pages, data)
                    .is_ok()
            } else {
                let data = unsafe { std::slice::from_raw_parts_mut(request.data, total_size) };
                disk_manager
                    .read_pages(request.page_id, request.num_pages, data)
                    .is_ok()
            }
        };

        // Signal completion
        if let Some(callback) = request.callback {
            let _ = callback.send(success);
        }
    }

    /// Returns a reference to the underlying DiskManager.
    pub fn disk_manager(&self) -> &Arc<DiskManager> {
        &self.disk_manager
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);

        // Wait for worker thread to finish
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_disk_scheduler_read_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
        let scheduler = DiskScheduler::new(dm);

        // Allocate a page
        let page_id = scheduler.disk_manager().allocate_page().unwrap();

        // Write data
        let mut write_data = [0u8; PAGE_SIZE];
        write_data[0] = 42;
        write_data[100] = 255;
        scheduler.schedule_write_sync(page_id, &write_data).unwrap();

        // Read it back
        let mut read_data = [0u8; PAGE_SIZE];
        scheduler
            .schedule_read_sync(page_id, &mut read_data)
            .unwrap();

        assert_eq!(read_data[0], 42);
        assert_eq!(read_data[100], 255);
    }

    #[test]
    fn test_disk_scheduler_multiple_requests() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
        let scheduler = DiskScheduler::new(dm);

        // Allocate multiple pages
        let page_id1 = scheduler.disk_manager().allocate_page().unwrap();
        let page_id2 = scheduler.disk_manager().allocate_page().unwrap();

        // Write to both pages
        let data1 = [1u8; PAGE_SIZE];
        let data2 = [2u8; PAGE_SIZE];

        scheduler.schedule_write_sync(page_id1, &data1).unwrap();
        scheduler.schedule_write_sync(page_id2, &data2).unwrap();

        // Read back
        let mut read1 = [0u8; PAGE_SIZE];
        let mut read2 = [0u8; PAGE_SIZE];

        scheduler.schedule_read_sync(page_id1, &mut read1).unwrap();
        scheduler.schedule_read_sync(page_id2, &mut read2).unwrap();

        assert_eq!(read1[0], 1);
        assert_eq!(read2[0], 2);
    }
}
