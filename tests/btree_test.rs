use std::sync::Arc;

use crio::buffer::BufferPoolManager;
use crio::common::{PageId, RecordId, SlotId};
use crio::index::BTreeIndex;
use crio::storage::disk::DiskManager;

use tempfile::NamedTempFile;

fn create_bpm(pool_size: usize) -> (Arc<BufferPoolManager>, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let disk_manager = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(pool_size, 2, disk_manager));
    (bpm, temp_file)
}

#[test]
fn test_btree_create() {
    let (bpm, _temp) = create_bpm(10);
    let index = BTreeIndex::new(bpm.clone()).unwrap();

    assert!(index.root_page_id().as_u32() > 0);
}

#[test]
fn test_btree_insert_and_search() {
    let (bpm, _temp) = create_bpm(10);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    let record1 = RecordId::new(PageId::new(100), SlotId::new(0));
    let record2 = RecordId::new(PageId::new(100), SlotId::new(1));
    let record3 = RecordId::new(PageId::new(101), SlotId::new(0));

    index.insert(10, record1).unwrap();
    index.insert(20, record2).unwrap();
    index.insert(30, record3).unwrap();

    assert_eq!(index.search(10).unwrap(), Some(record1));
    assert_eq!(index.search(20).unwrap(), Some(record2));
    assert_eq!(index.search(30).unwrap(), Some(record3));
    assert_eq!(index.search(40).unwrap(), None);
}

#[test]
fn test_btree_insert_many() {
    let (bpm, _temp) = create_bpm(50);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in 0..1000 {
        let record = RecordId::new(PageId::new(i), SlotId::new((i % 100) as u16));
        index.insert(i, record).unwrap();
    }

    for i in 0..1000 {
        let expected = RecordId::new(PageId::new(i), SlotId::new((i % 100) as u16));
        let result = index.search(i).unwrap();
        assert_eq!(result, Some(expected), "Failed to find key {}", i);
    }
}

#[test]
fn test_btree_insert_reverse() {
    let (bpm, _temp) = create_bpm(50);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in (0..100).rev() {
        let record = RecordId::new(PageId::new(i), SlotId::new(0));
        index.insert(i, record).unwrap();
    }

    for i in 0..100 {
        let expected = RecordId::new(PageId::new(i), SlotId::new(0));
        assert_eq!(index.search(i).unwrap(), Some(expected));
    }
}

#[test]
fn test_btree_range_scan() {
    let (bpm, _temp) = create_bpm(50);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in 0..100 {
        let record = RecordId::new(PageId::new(i), SlotId::new(0));
        index.insert(i * 10, record).unwrap();
    }

    let results = index.range_scan(200, 500).unwrap();

    assert_eq!(results.len(), 31); // 20, 21, ..., 50 (31 keys)

    for (i, (key, record)) in results.iter().enumerate() {
        let expected_key = (20 + i as u32) * 10;
        let expected_page_id = 20 + i as u32; // PageId matches the loop index, not the key
        let expected_record = RecordId::new(PageId::new(expected_page_id), SlotId::new(0));
        assert_eq!(*key, expected_key);
        assert_eq!(*record, expected_record);
    }
}

#[test]
fn test_btree_range_scan_empty() {
    let (bpm, _temp) = create_bpm(10);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in 0..10 {
        let record = RecordId::new(PageId::new(i), SlotId::new(0));
        index.insert(i, record).unwrap();
    }

    let results = index.range_scan(100, 200).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_btree_range_scan_all() {
    let (bpm, _temp) = create_bpm(50);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in 0..100 {
        let record = RecordId::new(PageId::new(i), SlotId::new(0));
        index.insert(i, record).unwrap();
    }

    let results = index.range_scan(0, 99).unwrap();
    assert_eq!(results.len(), 100);
}

#[test]
fn test_btree_split() {
    let (bpm, _temp) = create_bpm(100);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    for i in 0..200 {
        let record = RecordId::new(PageId::new(i), SlotId::new(0));
        index.insert(i, record).unwrap();
    }

    for i in 0..200 {
        let expected = RecordId::new(PageId::new(i), SlotId::new(0));
        assert_eq!(
            index.search(i).unwrap(),
            Some(expected),
            "Failed after split at key {}",
            i
        );
    }
}

#[test]
fn test_btree_random_insert() {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    let (bpm, _temp) = create_bpm(100);
    let mut index = BTreeIndex::new(bpm.clone()).unwrap();

    let mut keys: Vec<u32> = (0..500).collect();
    keys.shuffle(&mut thread_rng());

    for &key in &keys {
        let record = RecordId::new(PageId::new(key), SlotId::new(0));
        index.insert(key, record).unwrap();
    }

    for &key in &keys {
        let expected = RecordId::new(PageId::new(key), SlotId::new(0));
        assert_eq!(
            index.search(key).unwrap(),
            Some(expected),
            "Failed at key {}",
            key
        );
    }
}

#[test]
fn test_btree_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    let root_page_id = {
        let disk_manager = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(10, 2, disk_manager.clone()));
        let mut index = BTreeIndex::new(bpm.clone()).unwrap();

        for i in 0..50 {
            let record = RecordId::new(PageId::new(i), SlotId::new(0));
            index.insert(i, record).unwrap();
        }

        bpm.flush_all_pages().unwrap();
        index.root_page_id()
    };

    {
        let disk_manager = Arc::new(DiskManager::new(&path).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(10, 2, disk_manager));
        let index = BTreeIndex::open(root_page_id, bpm).unwrap();

        for i in 0..50 {
            let expected = RecordId::new(PageId::new(i), SlotId::new(0));
            assert_eq!(
                index.search(i).unwrap(),
                Some(expected),
                "Failed to find key {} after reload",
                i
            );
        }
    }
}
