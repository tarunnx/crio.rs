use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::common::{CrioError, PageId, Result, PAGE_SIZE};
use crate::storage::page::{DirectoryPage, DirectoryPageRef};

use super::extent_allocator::ExtentAllocator;

pub const DIRECTORY_PAGE_ID: PageId = PageId::new_const(0);

/// DiskManager is responsible for reading and writing pages to/from disk.
/// It manages multiple database files (segments) and tracks the number of pages allocated.
/// Supports both single-page and sequential multi-page I/O for performance.
/// Uses extent-based allocation to keep pages for the same table contiguous.
pub struct DiskManager {
    /// Map of FileID -> File Handle.
    /// Outer RwLock allows concurrent reads/writes to different files.
    /// Inner Mutex ensures exclusive access to the specific file cursor.
    files: RwLock<HashMap<u8, Mutex<File>>>,
    /// Base path for database files
    db_path: PathBuf,
    /// Total number of pages allocated across all files (approximate)
    num_pages: AtomicU32,
    /// Number of disk reads performed
    num_reads: AtomicU32,
    /// Number of disk writes performed
    num_writes: AtomicU32,
    /// Extent allocator for tracking free space
    extent_allocator: ExtentAllocator,
}

impl DiskManager {
    /// Creates a new DiskManager for the given database file path prefix.
    /// Scans for files named `db_path.0`, `db_path.1`, etc.
    /// If no files exist, creates `db_path.0` and initializes the directory page.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let mut files = HashMap::new();
        let mut total_pages = 0;
        let mut max_file_id = 0;

        // Try to open existing files starting from 0
        loop {
            let file_path = Self::get_segment_path(&db_path, max_file_id);
            if !file_path.exists() {
                break;
            }

            let file = OpenOptions::new().read(true).write(true).open(&file_path)?;

            let metadata = file.metadata()?;
            let file_size = metadata.len();
            let pages_in_file = (file_size / PAGE_SIZE as u64) as u32;

            files.insert(max_file_id, Mutex::new(file));
            total_pages += pages_in_file;
            max_file_id += 1;
        }

        // If no files found, create the first one (File 0)
        if files.is_empty() {
            let file_path = Self::get_segment_path(&db_path, 0);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?;

            files.insert(0, Mutex::new(file));
        }

        let extent_allocator = if total_pages > 0 {
            ExtentAllocator::from_existing(total_pages)
        } else {
            ExtentAllocator::new()
        };

        let dm = Self {
            files: RwLock::new(files),
            db_path,
            num_pages: AtomicU32::new(total_pages),
            num_reads: AtomicU32::new(0),
            num_writes: AtomicU32::new(0),
            extent_allocator,
        };

        // Initialize or validate directory page if we just created File 0 or it's empty
        if total_pages == 0 {
            dm.init_directory_page()?;
        } else {
            // Only validate if we have at least one page
            if total_pages > 0 {
                dm.validate_directory_page()?;
            }
        }

        Ok(dm)
    }

    /// Helper to construct segment file paths (e.g., "mydb.0", "mydb.1")
    fn get_segment_path(base_path: &Path, file_id: u8) -> PathBuf {
        // If base path has an extension, append .N to it.
        // Example: "test.db" -> "test.db.0"
        let mut path_str = base_path.to_string_lossy().to_string();
        path_str.push_str(&format!(".{}", file_id));
        PathBuf::from(path_str)
    }

    fn init_directory_page(&self) -> Result<()> {
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut dir_page = DirectoryPage::new(&mut data);
            dir_page.init();
        }

        self.num_pages.store(1, Ordering::SeqCst);

        // Directory page is always at PageId(0, 0) -> File 0, Offset 0
        let files = self.files.read();
        let mut file = files.get(&0).unwrap().lock();

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn validate_directory_page(&self) -> Result<()> {
        let mut data = [0u8; PAGE_SIZE];

        {
            let files = self.files.read();
            if let Some(mutex) = files.get(&0) {
                let mut file = mutex.lock();
                file.seek(SeekFrom::Start(0))?;
                // Handle case where file exists but is empty
                if let Ok(_) = file.read_exact(&mut data) {
                    let dir_page = DirectoryPageRef::new(&data);
                    if !dir_page.is_valid() {
                        return Err(CrioError::InvalidDatabaseFile);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn read_directory_page(&self, data: &mut [u8]) -> Result<()> {
        self.read_page(DIRECTORY_PAGE_ID, data)
    }

    pub fn write_directory_page(&self, data: &[u8]) -> Result<()> {
        self.write_page(DIRECTORY_PAGE_ID, data)
    }

    /// Adds a new file segment to the database.
    /// Returns the new File ID.
    pub fn add_file(&self) -> Result<u8> {
        let mut files = self.files.write();
        let next_file_id = files.len() as u8;

        // Hard limit check (since we use u8 for FileID)
        if next_file_id == u8::MAX {
            return Err(CrioError::DiskScheduler("Max files reached".to_string()));
        }

        let file_path = Self::get_segment_path(&self.db_path, next_file_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)?;

        files.insert(next_file_id, Mutex::new(file));
        Ok(next_file_id)
    }

    /// Reads a page from disk into the provided buffer.
    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let file_id = page_id.file_id();
        let page_offset = page_id.page_offset();
        let byte_offset = (page_offset as u64) * (PAGE_SIZE as u64);

        let files = self.files.read();
        let file_mutex = files
            .get(&file_id)
            .ok_or(CrioError::InvalidPageId(page_id))?;

        let mut file = file_mutex.lock();
        file.seek(SeekFrom::Start(byte_offset))?;

        let bytes_read = file.read(data)?;
        if bytes_read < PAGE_SIZE {
            data[bytes_read..].fill(0);
        }

        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes a page to disk from the provided buffer.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let file_id = page_id.file_id();
        let page_offset = page_id.page_offset();
        let byte_offset = (page_offset as u64) * (PAGE_SIZE as u64);

        let files = self.files.read();
        let file_mutex = files
            .get(&file_id)
            .ok_or(CrioError::InvalidPageId(page_id))?;

        let mut file = file_mutex.lock();
        file.seek(SeekFrom::Start(byte_offset))?;
        file.write_all(data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Reads multiple contiguous pages from disk in a single I/O operation.
    /// Note: This only works if all pages are within the SAME file.
    pub fn read_pages(&self, start_page_id: PageId, num_pages: u32, data: &mut [u8]) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(data.len(), expected_size);

        let file_id = start_page_id.file_id();
        let start_offset = start_page_id.page_offset();

        let end_offset = start_offset
            .checked_add(num_pages)
            .ok_or_else(|| CrioError::DiskScheduler("Page range overflow".to_string()))?;

        if end_offset > PageId::PAGE_OFFSET_MASK + 1 {
            return Err(CrioError::DiskScheduler(format!(
                "Sequential read crosses file boundary: start={}, count={}",
                start_offset, num_pages
            )));
        }

        let byte_offset = (start_offset as u64) * (PAGE_SIZE as u64);

        let files = self.files.read();
        let file_mutex = files
            .get(&file_id)
            .ok_or(CrioError::InvalidPageId(start_page_id))?;

        let mut file = file_mutex.lock();
        file.seek(SeekFrom::Start(byte_offset))?;

        let bytes_read = file.read(data)?;
        if bytes_read < expected_size {
            data[bytes_read..].fill(0);
        }

        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes multiple contiguous pages to disk.
    pub fn write_pages(&self, start_page_id: PageId, num_pages: u32, data: &[u8]) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(data.len(), expected_size);

        let file_id = start_page_id.file_id();
        let start_offset = start_page_id.page_offset();

        let end_offset = start_offset
            .checked_add(num_pages)
            .ok_or_else(|| CrioError::DiskScheduler("Page range overflow".to_string()))?;

        if end_offset > PageId::PAGE_OFFSET_MASK + 1 {
            return Err(CrioError::DiskScheduler(format!(
                "Sequential write crosses file boundary: start={}, count={}",
                start_offset, num_pages
            )));
        }

        let byte_offset = (start_offset as u64) * (PAGE_SIZE as u64);

        let files = self.files.read();
        let file_mutex = files
            .get(&file_id)
            .ok_or(CrioError::InvalidPageId(start_page_id))?;

        let mut file = file_mutex.lock();
        file.seek(SeekFrom::Start(byte_offset))?;
        file.write_all(data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Allocates a new page on disk and returns its page ID.
    /// Currently defaults to allocating in File 0.
    /// TODO: Implement logic to choose file based on free space or create new files.
    pub fn allocate_page(&self) -> Result<PageId> {
        // Simple allocation strategy: Linear growth in File 0
        // In a real system, we'd check free maps or file capacity.
        let raw_count = self.num_pages.fetch_add(1, Ordering::SeqCst);

        // For now, map everything to File 0.
        // Note: This relies on num_pages being consistent with File 0's size.
        // If we have multiple files, this simple counter logic breaks.
        // FIX: Use PageId::from_parts(0, raw_count) assuming raw_count tracks File 0 usage.

        let page_id = PageId::from_parts(0, raw_count);

        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    /// Allocates a new page for a specific table.
    pub fn allocate_page_for_table(&self, table_id: u32) -> Result<PageId> {
        let virtual_page_id = self.extent_allocator.allocate_page_for_table(table_id)?;

        let page_offset = virtual_page_id.as_u32();
        if page_offset > PageId::PAGE_OFFSET_MASK {
            return Err(CrioError::DiskScheduler(format!(
                "Page offset {} exceeds 24-bit limit",
                page_offset
            )));
        }

        let page_id = PageId::from_parts(0, page_offset);

        let required_pages = (page_offset + 1) as u32;
        let current_pages = self.num_pages.load(Ordering::Relaxed);
        if required_pages > current_pages {
            self.num_pages.store(required_pages, Ordering::SeqCst);
        }

        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    pub fn allocate_extent_for_table(&self, table_id: u32) -> Result<Vec<PageId>> {
        let virtual_pages = self.extent_allocator.allocate_extent_for_table(table_id)?;

        let mut real_pages = Vec::with_capacity(virtual_pages.len());

        for vp in &virtual_pages {
            let page_offset = vp.as_u32();
            if page_offset > PageId::PAGE_OFFSET_MASK {
                return Err(CrioError::DiskScheduler(format!(
                    "Page offset {} exceeds 24-bit limit",
                    page_offset
                )));
            }
        }

        if let Some(last_page) = virtual_pages.last() {
            let required_pages = last_page.as_u32() + 1;
            let current_pages = self.num_pages.load(Ordering::Relaxed);
            if required_pages > current_pages {
                self.num_pages.store(required_pages, Ordering::SeqCst);
            }
        }

        if let Some(first_virtual) = virtual_pages.first() {
            let start_page = PageId::from_parts(0, first_virtual.as_u32());
            let zeros = vec![0u8; PAGE_SIZE * virtual_pages.len()];
            self.write_pages(start_page, virtual_pages.len() as u32, &zeros)?;
        }

        for vp in virtual_pages {
            real_pages.push(PageId::from_parts(0, vp.as_u32()));
        }

        Ok(real_pages)
    }

    pub fn get_table_page_ranges(&self, table_id: u32) -> Vec<(PageId, u32)> {
        self.extent_allocator
            .get_contiguous_pages(table_id)
            .into_iter()
            .map(|(pid, count)| (PageId::from_parts(0, pid.as_u32()), count))
            .collect()
    }

    pub fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        // Map back to linear space for allocator
        // Only supports File 0 deallocation currently
        if page_id.file_id() == 0 {
            let virtual_pid = PageId::new(page_id.page_offset());
            self.extent_allocator.deallocate_page(virtual_pid);
        }
        Ok(())
    }

    pub fn get_num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Relaxed)
    }

    pub fn get_num_reads(&self) -> u32 {
        self.num_reads.load(Ordering::Relaxed)
    }

    pub fn get_num_writes(&self) -> u32 {
        self.num_writes.load(Ordering::Relaxed)
    }

    pub fn get_db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn sync(&self) -> Result<()> {
        let files = self.files.read();
        for file_mutex in files.values() {
            let file = file_mutex.lock();
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        let files = self.files.get_mut();
        for file_mutex in files.values_mut() {
            let file = file_mutex.get_mut();
            let _ = file.sync_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_manager_new() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let dm = DiskManager::new(&db_path).unwrap();

        // Should create test.db.0
        let segment_path = DiskManager::get_segment_path(&db_path, 0);
        assert!(segment_path.exists());
        assert_eq!(dm.get_num_pages(), 1); // Directory page
    }

    #[test]
    fn test_disk_manager_multi_file_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("multifile.db");
        let dm = DiskManager::new(&db_path).unwrap();

        // Add a second file (File 1)
        let file_id = dm.add_file().unwrap();
        assert_eq!(file_id, 1);

        let segment_path_1 = DiskManager::get_segment_path(&db_path, 1);
        assert!(segment_path_1.exists());

        // Write to File 1, Page 0
        let page_id = PageId::from_parts(1, 0);
        let mut data = [0u8; PAGE_SIZE];
        data[0] = 99;
        dm.write_page(page_id, &data).unwrap();

        // Read back
        let mut read_data = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_data).unwrap();
        assert_eq!(read_data[0], 99);
    }

    #[test]
    fn test_disk_manager_allocate_page() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("alloc.db");
        let dm = DiskManager::new(&db_path).unwrap();

        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id.file_id(), 0);
        assert_eq!(page_id.page_offset(), 1); // Page 0 is directory
    }

    #[test]
    fn test_disk_manager_read_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("rw.db");
        let dm = DiskManager::new(&db_path).unwrap();

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
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("persist.db");

        {
            let dm = DiskManager::new(&db_path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 123;
            dm.write_page(page_id, &data).unwrap();
        }

        {
            let dm = DiskManager::new(&db_path).unwrap();
            // Directory + 1 data page
            assert_eq!(dm.get_num_pages(), 2);

            let mut data = [0u8; PAGE_SIZE];
            // Read Page(0, 1)
            let page_id = PageId::from_parts(0, 1);
            dm.read_page(page_id, &mut data).unwrap();
            assert_eq!(data[0], 123);
        }
    }
}
