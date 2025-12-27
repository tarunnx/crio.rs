//! Integration tests for slotted page storage

use crio::common::{PageId, SlotId, PAGE_SIZE};
use crio::storage::page::{SlottedPage, TablePage};

#[test]
fn test_slotted_page_variable_length_tuples() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    // Insert tuples of varying sizes
    let tuples = [
        vec![1u8; 10],
        vec![2u8; 100],
        vec![3u8; 500],
        vec![4u8; 1000],
    ];

    let mut slot_ids = Vec::new();
    for tuple in &tuples {
        slot_ids.push(page.insert_tuple(tuple).unwrap());
    }

    // Verify all tuples
    for (i, tuple) in tuples.iter().enumerate() {
        assert_eq!(page.get_tuple(slot_ids[i]).unwrap(), tuple.as_slice());
    }
}

#[test]
fn test_slotted_page_fragmentation() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    // Insert tuples
    let slot1 = page.insert_tuple(&[1u8; 100]).unwrap();
    let slot2 = page.insert_tuple(&[2u8; 100]).unwrap();
    let slot3 = page.insert_tuple(&[3u8; 100]).unwrap();

    let initial_free = page.free_space();

    // Delete middle tuple
    page.delete_tuple(slot2).unwrap();

    // Free space should not increase (fragmentation)
    assert_eq!(page.free_space(), initial_free);

    // Compact should reclaim space
    page.compact();
    assert!(page.free_space() > initial_free);

    // Other tuples should still be readable
    assert_eq!(page.get_tuple(slot1).unwrap(), &[1u8; 100]);
    assert_eq!(page.get_tuple(slot3).unwrap(), &[3u8; 100]);
}

#[test]
fn test_slotted_page_slot_reuse() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    // Insert and delete
    let slot1 = page.insert_tuple(b"First").unwrap();
    let slot2 = page.insert_tuple(b"Second").unwrap();
    page.delete_tuple(slot1).unwrap();

    // Next insert should reuse slot1
    let slot3 = page.insert_tuple(b"Third").unwrap();
    assert_eq!(slot3, slot1);

    // Verify data
    assert_eq!(page.get_tuple(slot2).unwrap(), b"Second");
    assert_eq!(page.get_tuple(slot3).unwrap(), b"Third");
}

#[test]
fn test_slotted_page_maximum_capacity() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    let tuple = [0u8; 100]; // 100 bytes per tuple
    let mut count = 0;

    while page.can_insert(tuple.len()) {
        page.insert_tuple(&tuple).unwrap();
        count += 1;
    }

    // Should have inserted many tuples
    assert!(count > 30); // At least 30 tuples of 100 bytes each

    // Verify we really can't insert more
    assert!(!page.can_insert(tuple.len()));
}

#[test]
fn test_slotted_page_update_smaller() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    let slot_id = page.insert_tuple(b"Hello, World!").unwrap();

    // Update with smaller data
    page.update_tuple(slot_id, b"Hi").unwrap();
    assert_eq!(page.get_tuple(slot_id).unwrap(), b"Hi");
}

#[test]
fn test_slotted_page_update_same_size() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    let slot_id = page.insert_tuple(b"Hello").unwrap();

    // Update with same size data
    page.update_tuple(slot_id, b"World").unwrap();
    assert_eq!(page.get_tuple(slot_id).unwrap(), b"World");
}

#[test]
fn test_slotted_page_update_too_large_fails() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    let slot_id = page.insert_tuple(b"Hi").unwrap();

    // Update with larger data should fail
    let result = page.update_tuple(slot_id, b"Hello, World!");
    assert!(result.is_err());

    // Original data should be unchanged
    assert_eq!(page.get_tuple(slot_id).unwrap(), b"Hi");
}

#[test]
fn test_table_page_linked_list() {
    let mut data1 = [0u8; PAGE_SIZE];
    let mut data2 = [0u8; PAGE_SIZE];
    let mut data3 = [0u8; PAGE_SIZE];

    let mut page1 = TablePage::new(&mut data1);
    let mut page2 = TablePage::new(&mut data2);
    let mut page3 = TablePage::new(&mut data3);

    page1.init(PageId::new(0), 1);
    page2.init(PageId::new(1), 1);
    page3.init(PageId::new(2), 1);

    // Link pages: 1 <-> 2 <-> 3
    page1.set_next_page_id(Some(PageId::new(1)));

    page2.set_prev_page_id(Some(PageId::new(0)));
    page2.set_next_page_id(Some(PageId::new(2)));

    page3.set_prev_page_id(Some(PageId::new(1)));

    // Verify links
    assert_eq!(page1.prev_page_id(), None);
    assert_eq!(page1.next_page_id(), Some(PageId::new(1)));

    assert_eq!(page2.prev_page_id(), Some(PageId::new(0)));
    assert_eq!(page2.next_page_id(), Some(PageId::new(2)));

    assert_eq!(page3.prev_page_id(), Some(PageId::new(1)));
    assert_eq!(page3.next_page_id(), None);
}

#[test]
fn test_table_page_record_iteration() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut data);
    page.init(PageId::new(0), 1);

    // Insert tuples
    for i in 0..5 {
        let tuple = format!("Tuple {}", i);
        page.insert_tuple(tuple.as_bytes()).unwrap();
    }

    // Iterate and verify
    let collected: Vec<_> = page.record_ids().collect();
    assert_eq!(collected.len(), 5);

    for (i, rid) in collected.iter().enumerate() {
        assert_eq!(rid.page_id, PageId::new(0));
        assert_eq!(rid.slot_id, SlotId::new(i as u16));

        let expected = format!("Tuple {}", i);
        assert_eq!(page.get_tuple(rid.slot_id).unwrap(), expected.as_bytes());
    }
}

#[test]
fn test_table_page_compact_preserves_slot_ids() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut data);
    page.init(PageId::new(0), 1);

    // Insert tuples
    let rid0 = page.insert_tuple(b"Tuple 0").unwrap();
    let rid1 = page.insert_tuple(b"Tuple 1").unwrap();
    let rid2 = page.insert_tuple(b"Tuple 2").unwrap();

    // Delete middle tuple
    page.delete_tuple(rid1.slot_id).unwrap();

    // Compact
    page.compact();

    // Slot IDs should be preserved
    assert_eq!(page.get_tuple(rid0.slot_id).unwrap(), b"Tuple 0");
    assert!(page.get_tuple(rid1.slot_id).is_err()); // Deleted
    assert_eq!(page.get_tuple(rid2.slot_id).unwrap(), b"Tuple 2");
}

#[test]
fn test_slotted_page_single_byte_tuple() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    // Insert single-byte tuple (empty tuples are not supported since length=0 means deleted)
    let slot_id = page.insert_tuple(&[42]).unwrap();

    // Should be retrievable
    let tuple = page.get_tuple(slot_id).unwrap();
    assert_eq!(tuple, &[42]);
}

#[test]
fn test_slotted_page_large_tuple() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    // Insert a tuple that takes up most of the page
    let large_tuple = vec![0xABu8; PAGE_SIZE - 100]; // Leave some room for header

    if page.can_insert(large_tuple.len()) {
        let slot_id = page.insert_tuple(&large_tuple).unwrap();
        assert_eq!(page.get_tuple(slot_id).unwrap(), large_tuple.as_slice());
    }
}

#[test]
fn test_slotted_page_tuple_count() {
    let mut data = [0u8; PAGE_SIZE];
    let mut page = SlottedPage::new(&mut data);
    page.init(PageId::new(0));

    assert_eq!(page.tuple_count(), 0);

    let slot1 = page.insert_tuple(b"One").unwrap();
    assert_eq!(page.tuple_count(), 1);

    let slot2 = page.insert_tuple(b"Two").unwrap();
    assert_eq!(page.tuple_count(), 2);

    page.delete_tuple(slot1).unwrap();
    assert_eq!(page.tuple_count(), 1);

    page.delete_tuple(slot2).unwrap();
    assert_eq!(page.tuple_count(), 0);
}
