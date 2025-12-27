//! Integration tests for the buffer pool manager

use std::sync::Arc;
use std::thread;

use crio::buffer::BufferPoolManager;
use crio::common::{CrioError, PageId};
use crio::storage::disk::DiskManager;
use crio::storage::page::TablePage;
use tempfile::NamedTempFile;

fn create_bpm(pool_size: usize) -> (BufferPoolManager, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let bpm = BufferPoolManager::new(pool_size, 2, dm);
    (bpm, temp_file)
}

#[test]
fn test_buffer_pool_basic_operations() {
    let (bpm, _temp) = create_bpm(10);

    // Create a new page (page 0 is directory, so first data page is 1)
    let page_id = bpm.new_page().unwrap();
    assert_eq!(page_id, PageId::new(1));

    // Write data to the page
    {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        guard.data_mut()[0] = 0xDE;
        guard.data_mut()[1] = 0xAD;
        guard.data_mut()[2] = 0xBE;
        guard.data_mut()[3] = 0xEF;
    }

    // Read data back
    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        assert_eq!(guard.data()[0], 0xDE);
        assert_eq!(guard.data()[1], 0xAD);
        assert_eq!(guard.data()[2], 0xBE);
        assert_eq!(guard.data()[3], 0xEF);
    }
}

#[test]
fn test_buffer_pool_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    let page_id;
    let test_data = b"Persistence test data";

    // Write data
    {
        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = BufferPoolManager::new(10, 2, dm);

        page_id = bpm.new_page().unwrap();

        {
            let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
            guard.data_mut()[..test_data.len()].copy_from_slice(test_data);
        }

        bpm.flush_page(page_id).unwrap();
    }

    // Read data back with a new BPM
    {
        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = BufferPoolManager::new(10, 2, dm);

        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        assert_eq!(&guard.data()[..test_data.len()], test_data);
    }
}

#[test]
fn test_buffer_pool_eviction() {
    let (bpm, _temp) = create_bpm(3);

    // Fill the buffer pool
    let mut page_ids = Vec::new();
    for i in 0..3 {
        let pid = bpm.new_page().unwrap();
        {
            let mut guard = bpm.checked_write_page(pid).unwrap().unwrap();
            guard.data_mut()[0] = i as u8;
        }
        page_ids.push(pid);
    }

    // All pages should be unpinned now
    for &pid in &page_ids {
        assert_eq!(bpm.get_pin_count(pid), Some(0));
    }

    // Creating a new page should evict one
    let new_pid = bpm.new_page().unwrap();
    assert_eq!(new_pid, PageId::new(4)); // Pages 1,2,3 exist, new is 4

    // The evicted page's data should still be on disk
    // Access all original pages - one will be fetched from disk
    for (i, &pid) in page_ids.iter().enumerate() {
        let guard = bpm.checked_read_page(pid).unwrap().unwrap();
        assert_eq!(guard.data()[0], i as u8);
    }
}

#[test]
fn test_buffer_pool_pin_prevents_eviction() {
    let (bpm, _temp) = create_bpm(2);

    // Allocate two pages
    let pid1 = bpm.new_page().unwrap();
    let pid2 = bpm.new_page().unwrap();

    // Keep both pages pinned
    let _guard1 = bpm.checked_read_page(pid1).unwrap().unwrap();
    let _guard2 = bpm.checked_read_page(pid2).unwrap().unwrap();

    // Trying to create a new page should fail
    let result = bpm.new_page();
    assert!(matches!(result, Err(CrioError::BufferPoolFull)));
}

#[test]
fn test_buffer_pool_delete_page() {
    let (bpm, _temp) = create_bpm(10);

    let pid = bpm.new_page().unwrap();

    // Write some data
    {
        let mut guard = bpm.checked_write_page(pid).unwrap().unwrap();
        guard.data_mut()[0] = 42;
    }

    // Delete the page
    assert!(bpm.delete_page(pid).unwrap());

    // The page should no longer be in the buffer pool
    assert_eq!(bpm.get_pin_count(pid), None);
}

#[test]
fn test_buffer_pool_cannot_delete_pinned_page() {
    let (bpm, _temp) = create_bpm(10);

    let pid = bpm.new_page().unwrap();
    let _guard = bpm.checked_read_page(pid).unwrap().unwrap();

    // Cannot delete while pinned
    let result = bpm.delete_page(pid);
    assert!(matches!(result, Err(CrioError::PageStillPinned(_))));
}

#[test]
fn test_buffer_pool_flush_all() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    let page_ids;

    // Write data to multiple pages
    {
        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = BufferPoolManager::new(10, 2, dm);

        page_ids = (0..5)
            .map(|i| {
                let pid = bpm.new_page().unwrap();
                {
                    let mut guard = bpm.checked_write_page(pid).unwrap().unwrap();
                    guard.data_mut()[0] = i as u8;
                }
                pid
            })
            .collect::<Vec<_>>();

        bpm.flush_all_pages().unwrap();
    }

    // Read back with new BPM
    {
        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = BufferPoolManager::new(10, 2, dm);

        for (i, &pid) in page_ids.iter().enumerate() {
            let guard = bpm.checked_read_page(pid).unwrap().unwrap();
            assert_eq!(guard.data()[0], i as u8);
        }
    }
}

#[test]
fn test_buffer_pool_concurrent_access() {
    let (bpm, _temp) = create_bpm(10);
    let bpm = Arc::new(bpm);

    // Create a page
    let page_id = bpm.new_page().unwrap();

    // Spawn multiple reader threads
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let bpm = Arc::clone(&bpm);
            thread::spawn(move || {
                for _ in 0..100 {
                    let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
                    let _ = guard.data()[0]; // Just read
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_buffer_pool_with_table_pages() {
    let (bpm, _temp) = create_bpm(10);

    let page_id = bpm.new_page().unwrap();

    // Initialize as a table page and insert tuples
    {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1);

        page.insert_tuple(b"First tuple").unwrap();
        page.insert_tuple(b"Second tuple").unwrap();
        page.insert_tuple(b"Third tuple").unwrap();

        assert_eq!(page.tuple_count(), 3);
    }

    // Read back the tuples
    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        let page = crio::storage::page::TablePageRef::new(guard.data());

        assert_eq!(page.tuple_count(), 3);
        assert_eq!(
            page.get_tuple(crio::SlotId::new(0)).unwrap(),
            b"First tuple"
        );
        assert_eq!(
            page.get_tuple(crio::SlotId::new(1)).unwrap(),
            b"Second tuple"
        );
        assert_eq!(
            page.get_tuple(crio::SlotId::new(2)).unwrap(),
            b"Third tuple"
        );
    }
}

#[test]
fn test_buffer_pool_large_workload() {
    let (bpm, _temp) = create_bpm(5); // Small pool to force evictions

    // Create many pages
    let page_ids: Vec<_> = (0..20).map(|_| bpm.new_page().unwrap()).collect();

    // Write to each page
    for &pid in &page_ids {
        let mut guard = bpm.checked_write_page(pid).unwrap().unwrap();
        let id_bytes = pid.as_u32().to_le_bytes();
        guard.data_mut()[..4].copy_from_slice(&id_bytes);
    }

    // Read from each page and verify
    for &pid in &page_ids {
        let guard = bpm.checked_read_page(pid).unwrap().unwrap();
        let id_bytes: [u8; 4] = guard.data()[..4].try_into().unwrap();
        assert_eq!(u32::from_le_bytes(id_bytes), pid.as_u32());
    }
}
