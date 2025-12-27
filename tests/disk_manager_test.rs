//! Integration tests for the disk manager and scheduler

use std::sync::Arc;
use std::thread;

use crio::common::{PageId, PAGE_SIZE};
use crio::storage::disk::{DiskManager, DiskScheduler};
use tempfile::NamedTempFile;

#[test]
fn test_disk_manager_create_file() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    assert_eq!(dm.get_num_pages(), 1); // Directory page at 0
    assert_eq!(dm.get_num_reads(), 0);
    assert_eq!(dm.get_num_writes(), 1); // Directory page write
}

#[test]
fn test_disk_manager_allocate_pages() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    for i in 0..10 {
        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(i + 1)); // Page 0 is directory
    }

    assert_eq!(dm.get_num_pages(), 11); // 1 directory + 10 data pages
}

#[test]
fn test_disk_manager_read_write_page() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    let page_id = dm.allocate_page().unwrap();

    // Write pattern
    let mut write_data = [0u8; PAGE_SIZE];
    for i in 0..PAGE_SIZE {
        write_data[i] = (i % 256) as u8;
    }
    dm.write_page(page_id, &write_data).unwrap();

    // Read back
    let mut read_data = [0u8; PAGE_SIZE];
    dm.read_page(page_id, &mut read_data).unwrap();

    assert_eq!(write_data, read_data);
}

#[test]
fn test_disk_manager_random_access() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    // Allocate 10 pages
    let page_ids: Vec<_> = (0..10).map(|_| dm.allocate_page().unwrap()).collect();

    // Write to pages in random order
    let write_order = [5, 2, 8, 0, 7, 3, 9, 1, 6, 4];
    for &i in &write_order {
        let mut data = [0u8; PAGE_SIZE];
        data[0] = i as u8;
        dm.write_page(page_ids[i], &data).unwrap();
    }

    // Read back and verify
    for (i, &page_id) in page_ids.iter().enumerate() {
        let mut data = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut data).unwrap();
        assert_eq!(data[0], i as u8);
    }
}

#[test]
fn test_disk_manager_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    let test_data = b"Persistence test";

    // Write data
    {
        let dm = DiskManager::new(&path).unwrap();
        let page_id = dm.allocate_page().unwrap();

        let mut data = [0u8; PAGE_SIZE];
        data[..test_data.len()].copy_from_slice(test_data);
        dm.write_page(page_id, &data).unwrap();
        dm.sync().unwrap();
    }

    // Read back with a new DiskManager
    {
        let dm = DiskManager::new(&path).unwrap();
        assert_eq!(dm.get_num_pages(), 2); // 1 directory + 1 data page

        let mut data = [0u8; PAGE_SIZE];
        dm.read_page(PageId::new(1), &mut data).unwrap(); // Data page is at 1
        assert_eq!(&data[..test_data.len()], test_data);
    }
}

#[test]
fn test_disk_scheduler_basic() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let scheduler = DiskScheduler::new(dm);

    let page_id = scheduler.disk_manager().allocate_page().unwrap();

    // Write via scheduler
    let mut data = [0u8; PAGE_SIZE];
    data[0] = 42;
    scheduler.schedule_write_sync(page_id, &data).unwrap();

    // Read via scheduler
    let mut read_data = [0u8; PAGE_SIZE];
    scheduler
        .schedule_read_sync(page_id, &mut read_data)
        .unwrap();

    assert_eq!(read_data[0], 42);
}

#[test]
fn test_disk_scheduler_multiple_requests() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let scheduler = DiskScheduler::new(dm);

    // Allocate multiple pages
    let page_ids: Vec<_> = (0..5)
        .map(|_| scheduler.disk_manager().allocate_page().unwrap())
        .collect();

    // Write to all pages
    for &pid in &page_ids {
        let mut data = [0u8; PAGE_SIZE];
        data[0] = pid.as_u32() as u8;
        scheduler.schedule_write_sync(pid, &data).unwrap();
    }

    // Read back and verify
    for &pid in &page_ids {
        let mut data = [0u8; PAGE_SIZE];
        scheduler.schedule_read_sync(pid, &mut data).unwrap();
        assert_eq!(data[0], pid.as_u32() as u8);
    }
}

#[test]
fn test_disk_scheduler_concurrent_requests() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let scheduler = Arc::new(DiskScheduler::new(dm));

    // Pre-allocate pages
    let page_ids: Vec<_> = (0..10)
        .map(|_| scheduler.disk_manager().allocate_page().unwrap())
        .collect();

    // Spawn threads to write concurrently
    let handles: Vec<_> = page_ids
        .iter()
        .map(|&pid| {
            let scheduler = Arc::clone(&scheduler);
            thread::spawn(move || {
                let mut data = [0u8; PAGE_SIZE];
                data[0] = pid.as_u32() as u8;
                scheduler.schedule_write_sync(pid, &data).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes
    for &pid in &page_ids {
        let mut data = [0u8; PAGE_SIZE];
        scheduler.schedule_read_sync(pid, &mut data).unwrap();
        assert_eq!(data[0], pid.as_u32() as u8);
    }
}

#[test]
fn test_disk_manager_io_stats() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    assert_eq!(dm.get_num_reads(), 0);
    assert_eq!(dm.get_num_writes(), 1); // Directory page write on init

    let page_id = dm.allocate_page().unwrap();
    assert_eq!(dm.get_num_writes(), 2); // allocate_page writes zeros

    let data = [0u8; PAGE_SIZE];
    dm.write_page(page_id, &data).unwrap();
    assert_eq!(dm.get_num_writes(), 3);

    let mut read_data = [0u8; PAGE_SIZE];
    dm.read_page(page_id, &mut read_data).unwrap();
    assert_eq!(dm.get_num_reads(), 1);
}

#[test]
fn test_disk_manager_large_file() {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = DiskManager::new(temp_file.path()).unwrap();

    // Allocate many pages (100 pages = 400 KB)
    let page_ids: Vec<_> = (0..100).map(|_| dm.allocate_page().unwrap()).collect();

    // Write to all pages
    for &pid in &page_ids {
        let mut data = [0u8; PAGE_SIZE];
        let id_bytes = pid.as_u32().to_le_bytes();
        data[..4].copy_from_slice(&id_bytes);
        dm.write_page(pid, &data).unwrap();
    }

    // Read back and verify
    for &pid in &page_ids {
        let mut data = [0u8; PAGE_SIZE];
        dm.read_page(pid, &mut data).unwrap();
        let id_bytes: [u8; 4] = data[..4].try_into().unwrap();
        assert_eq!(u32::from_le_bytes(id_bytes), pid.as_u32());
    }
}
