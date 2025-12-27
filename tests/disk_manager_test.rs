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

#[test]
fn test_page_id_encoding() {
    assert_eq!(PageId::from_parts(0, 0).file_id(), 0);
    assert_eq!(PageId::from_parts(0, 0).page_offset(), 0);

    assert_eq!(PageId::from_parts(1, 100).file_id(), 1);
    assert_eq!(PageId::from_parts(1, 100).page_offset(), 100);

    assert_eq!(PageId::from_parts(255, 0x00FFFFFF).file_id(), 255);
    assert_eq!(
        PageId::from_parts(255, 0x00FFFFFF).page_offset(),
        0x00FFFFFF
    );

    let max_page = PageId::from_parts(255, PageId::PAGE_OFFSET_MASK);
    assert_eq!(max_page.as_u32(), 0xFFFFFFFF);
}

#[test]
#[should_panic(expected = "Page offset too large")]
fn test_page_id_overflow_panics() {
    PageId::from_parts(0, PageId::PAGE_OFFSET_MASK + 1);
}

#[test]
fn test_multi_file_isolation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multi.db");
    let dm = DiskManager::new(&db_path).unwrap();

    let file1_id = dm.add_file().unwrap();
    assert_eq!(file1_id, 1);

    let page_f0 = PageId::from_parts(0, 5);
    let page_f1 = PageId::from_parts(1, 5);

    let mut data_f0 = [0u8; PAGE_SIZE];
    data_f0[0] = 111;
    dm.write_page(page_f0, &data_f0).unwrap();

    let mut data_f1 = [0u8; PAGE_SIZE];
    data_f1[0] = 222;
    dm.write_page(page_f1, &data_f1).unwrap();

    let mut read_f0 = [0u8; PAGE_SIZE];
    dm.read_page(page_f0, &mut read_f0).unwrap();
    assert_eq!(read_f0[0], 111);

    let mut read_f1 = [0u8; PAGE_SIZE];
    dm.read_page(page_f1, &mut read_f1).unwrap();
    assert_eq!(read_f1[0], 222);
}

#[test]
fn test_multi_file_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("persist_multi.db");

    {
        let dm = DiskManager::new(&db_path).unwrap();
        dm.add_file().unwrap();
        dm.add_file().unwrap();

        let page0 = PageId::from_parts(0, 2);
        let page1 = PageId::from_parts(1, 0);
        let page2 = PageId::from_parts(2, 0);

        let mut data = [0u8; PAGE_SIZE];
        data[0] = 10;
        dm.write_page(page0, &data).unwrap();

        data[0] = 20;
        dm.write_page(page1, &data).unwrap();

        data[0] = 30;
        dm.write_page(page2, &data).unwrap();

        dm.sync().unwrap();
    }

    {
        let dm = DiskManager::new(&db_path).unwrap();

        let mut data = [0u8; PAGE_SIZE];

        dm.read_page(PageId::from_parts(0, 2), &mut data).unwrap();
        assert_eq!(data[0], 10);

        dm.read_page(PageId::from_parts(1, 0), &mut data).unwrap();
        assert_eq!(data[0], 20);

        dm.read_page(PageId::from_parts(2, 0), &mut data).unwrap();
        assert_eq!(data[0], 30);
    }
}

#[test]
fn test_sequential_io_within_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("seq.db");
    let dm = DiskManager::new(&db_path).unwrap();

    let start_page = PageId::from_parts(0, 10);
    let num_pages = 5;

    let mut write_data = vec![0u8; PAGE_SIZE * num_pages];
    for i in 0..num_pages {
        write_data[i * PAGE_SIZE] = (i + 100) as u8;
    }

    dm.write_pages(start_page, num_pages as u32, &write_data)
        .unwrap();

    let mut read_data = vec![0u8; PAGE_SIZE * num_pages];
    dm.read_pages(start_page, num_pages as u32, &mut read_data)
        .unwrap();

    for i in 0..num_pages {
        assert_eq!(read_data[i * PAGE_SIZE], (i + 100) as u8);
    }
}

#[test]
fn test_sequential_io_boundary_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("boundary.db");
    let dm = DiskManager::new(&db_path).unwrap();

    let near_limit = PageId::PAGE_OFFSET_MASK - 10;
    let start_page = PageId::from_parts(0, near_limit);

    let too_many_pages = 20;
    let data = vec![0u8; PAGE_SIZE * too_many_pages];

    let result = dm.write_pages(start_page, too_many_pages as u32, &data);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("file boundary"));
}

#[test]
fn test_read_from_nonexistent_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("nofile.db");
    let dm = DiskManager::new(&db_path).unwrap();

    let page_in_file5 = PageId::from_parts(5, 0);
    let mut data = [0u8; PAGE_SIZE];

    let result = dm.read_page(page_in_file5, &mut data);
    assert!(result.is_err());
}

#[test]
fn test_concurrent_multi_file_access() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("concurrent.db");
    let dm = Arc::new(DiskManager::new(&db_path).unwrap());

    dm.add_file().unwrap();
    dm.add_file().unwrap();

    let handles: Vec<_> = (0..3)
        .map(|file_id| {
            let dm_clone = Arc::clone(&dm);
            thread::spawn(move || {
                for i in 0..10 {
                    let page_id = PageId::from_parts(file_id as u8, i);
                    let mut data = [0u8; PAGE_SIZE];
                    data[0] = (file_id * 100 + i) as u8;
                    dm_clone.write_page(page_id, &data).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    for file_id in 0..3 {
        for i in 0..10 {
            let page_id = PageId::from_parts(file_id as u8, i);
            let mut data = [0u8; PAGE_SIZE];
            dm.read_page(page_id, &mut data).unwrap();
            assert_eq!(data[0], (file_id * 100 + i) as u8);
        }
    }
}
