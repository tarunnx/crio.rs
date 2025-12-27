use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::common::{PageId, Result, PAGE_SIZE};

/// DiskManager is responsible for reading and writing pages to/from disk.
/// It manages a single database file and tracks the number of pages allocated.
pub struct DiskManager {
    /// The database file
    db_file: Mutex<File>,
    /// Path to the database file
    db_path: String,
    /// Number of pages currently allocated
    num_pages: AtomicU32,
    /// Number of disk reads performed
    num_reads: AtomicU32,
    /// Number of disk writes performed
    num_writes: AtomicU32,
}

impl DiskManager {
    /// Creates a new DiskManager for the given database file path.
    /// Creates the file if it doesn't exist.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path_str = db_path.as_ref().to_string_lossy().to_string();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&db_path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = (file_size / PAGE_SIZE as u64) as u32;

        Ok(Self {
            db_file: Mutex::new(file),
            db_path: path_str,
            num_pages: AtomicU32::new(num_pages),
            num_reads: AtomicU32::new(0),
            num_writes: AtomicU32::new(0),
        })
    }

    /// Reads a page from disk into the provided buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let offset = (page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;

        // If we're reading beyond the file, fill with zeros
        let bytes_read = file.read(data)?;
        if bytes_read < PAGE_SIZE {
            data[bytes_read..].fill(0);
        }

        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes a page to disk from the provided buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let offset = (page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Allocates a new page on disk and returns its page ID.
    /// The page is zero-initialized.
    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = PageId::new(self.num_pages.fetch_add(1, Ordering::SeqCst));

        // Zero-initialize the new page
        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    /// Deallocates a page. In this simple implementation, we don't actually
    /// reclaim the space - we just note that the page is no longer in use.
    /// A more sophisticated implementation would maintain a free list.
    pub fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        // In a real implementation, we would add this page to a free list
        // For now, we just leave it allocated but unused
        Ok(())
    }

    /// Returns the number of pages currently allocated.
    pub fn get_num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Relaxed)
    }

    /// Returns the number of disk reads performed.
    pub fn get_num_reads(&self) -> u32 {
        self.num_reads.load(Ordering::Relaxed)
    }

    /// Returns the number of disk writes performed.
    pub fn get_num_writes(&self) -> u32 {
        self.num_writes.load(Ordering::Relaxed)
    }

    /// Returns the path to the database file.
    pub fn get_db_path(&self) -> &str {
        &self.db_path
    }

    /// Flushes any buffered writes to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.db_file.lock();
        file.sync_all()?;
        Ok(())
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        // Ensure all data is flushed to disk
        let file = self.db_file.get_mut();
        let _ = file.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_disk_manager_new() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        assert_eq!(dm.get_num_pages(), 0);
    }

    #[test]
    fn test_disk_manager_allocate_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(0));
        assert_eq!(dm.get_num_pages(), 1);

        let page_id2 = dm.allocate_page().unwrap();
        assert_eq!(page_id2, PageId::new(1));
        assert_eq!(dm.get_num_pages(), 2);
    }

    #[test]
    fn test_disk_manager_read_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page().unwrap();

        // Write data
        let mut write_data = [0u8; PAGE_SIZE];
        write_data[0] = 42;
        write_data[100] = 255;
        write_data[PAGE_SIZE - 1] = 128;
        dm.write_page(page_id, &write_data).unwrap();

        // Read it back
        let mut read_data = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_data).unwrap();

        assert_eq!(read_data[0], 42);
        assert_eq!(read_data[100], 255);
        assert_eq!(read_data[PAGE_SIZE - 1], 128);
    }

    #[test]
    fn test_disk_manager_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write data
        {
            let dm = DiskManager::new(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 123;
            dm.write_page(page_id, &data).unwrap();
        }

        // Read it back with a new DiskManager
        {
            let dm = DiskManager::new(&path).unwrap();
            assert_eq!(dm.get_num_pages(), 1);

            let mut data = [0u8; PAGE_SIZE];
            dm.read_page(PageId::new(0), &mut data).unwrap();
            assert_eq!(data[0], 123);
        }
    }
}
