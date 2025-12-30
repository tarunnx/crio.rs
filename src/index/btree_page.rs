use crate::common::{PageId, RecordId, Result, SlotId, PAGE_SIZE};

const HEADER_SIZE: usize = 20;

const PAGE_ID_OFFSET: usize = 0;
const IS_LEAF_OFFSET: usize = 4;
const NUM_KEYS_OFFSET: usize = 5;
const NEXT_PAGE_OFFSET: usize = 8;
const PREV_PAGE_OFFSET: usize = 12;
const PARENT_PAGE_OFFSET: usize = 16;

const INVALID_PAGE: u32 = u32::MAX;

const KEY_SIZE: usize = 4; // u32 keys
const VALUE_SIZE: usize = 6; // RecordId: PageId(4) + SlotId(2)
const CHILD_SIZE: usize = 4; // PageId

#[derive(Debug, Clone, Copy)]
pub struct KeyValuePair {
    pub key: u32,
    pub value: RecordId,
}

pub struct BTreeNode<'a> {
    data: &'a mut [u8],
}

impl<'a> BTreeNode<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    pub fn init(&mut self, page_id: PageId, is_leaf: bool) {
        self.data.fill(0);
        self.set_page_id(page_id);
        self.set_is_leaf(is_leaf);
        self.set_num_keys(0);
        self.set_next_page_id(None);
        self.set_prev_page_id(None);
        self.set_parent_page_id(None);
    }

    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 4] = self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    fn set_page_id(&mut self, page_id: PageId) {
        let bytes = page_id.as_u32().to_le_bytes();
        self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4].copy_from_slice(&bytes);
    }

    pub fn is_leaf(&self) -> bool {
        self.data[IS_LEAF_OFFSET] == 1
    }

    fn set_is_leaf(&mut self, is_leaf: bool) {
        self.data[IS_LEAF_OFFSET] = if is_leaf { 1 } else { 0 };
    }

    pub fn num_keys(&self) -> u16 {
        let bytes: [u8; 2] = self.data[NUM_KEYS_OFFSET..NUM_KEYS_OFFSET + 2]
            .try_into()
            .unwrap();
        u16::from_le_bytes(bytes)
    }

    fn set_num_keys(&mut self, num: u16) {
        let bytes = num.to_le_bytes();
        self.data[NUM_KEYS_OFFSET..NUM_KEYS_OFFSET + 2].copy_from_slice(&bytes);
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.data[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        let value = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        let bytes = value.to_le_bytes();
        self.data[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4].copy_from_slice(&bytes);
    }

    pub fn prev_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.data[PREV_PAGE_OFFSET..PREV_PAGE_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    pub fn set_prev_page_id(&mut self, page_id: Option<PageId>) {
        let value = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        let bytes = value.to_le_bytes();
        self.data[PREV_PAGE_OFFSET..PREV_PAGE_OFFSET + 4].copy_from_slice(&bytes);
    }

    pub fn parent_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.data[PARENT_PAGE_OFFSET..PARENT_PAGE_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    pub fn set_parent_page_id(&mut self, page_id: Option<PageId>) {
        let value = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        let bytes = value.to_le_bytes();
        self.data[PARENT_PAGE_OFFSET..PARENT_PAGE_OFFSET + 4].copy_from_slice(&bytes);
    }

    pub fn get_key(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * KEY_SIZE;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    fn set_key(&mut self, index: usize, key: u32) {
        let offset = HEADER_SIZE + index * KEY_SIZE;
        let bytes = key.to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
    }

    pub fn get_value(&self, index: usize) -> RecordId {
        self.get_value_at(index, self.num_keys() as usize)
    }

    pub fn get_child(&self, index: usize) -> PageId {
        self.get_child_at(index, self.num_keys() as usize)
    }

    pub fn search_key(&self, key: u32) -> usize {
        let num_keys = self.num_keys() as usize;
        let mut left = 0;
        let mut right = num_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key(mid);

            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }

    pub fn insert_key_value(&mut self, key: u32, value: RecordId) -> Result<()> {
        let num_keys = self.num_keys() as usize;
        let pos = self.search_key(key);

        // Read all existing values before modifying anything
        let mut values: Vec<RecordId> = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            values.push(self.get_value_at(i, num_keys));
        }

        // Shift keys
        for i in (pos..num_keys).rev() {
            let k = self.get_key(i);
            self.set_key(i + 1, k);
        }

        // Update num_keys
        self.set_num_keys((num_keys + 1) as u16);

        // Set the new key
        self.set_key(pos, key);

        // Write all values to their new positions (all shift due to key array growing)
        for i in 0..num_keys {
            if i >= pos {
                self.set_value_at(i + 1, values[i], num_keys + 1);
            } else {
                self.set_value_at(i, values[i], num_keys + 1);
            }
        }

        // Set the new value
        self.set_value_at(pos, value, num_keys + 1);

        Ok(())
    }

    fn get_value_at(&self, index: usize, num_keys: usize) -> RecordId {
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * VALUE_SIZE;

        let page_id_bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        let slot_id_bytes: [u8; 2] = self.data[offset + 4..offset + 6].try_into().unwrap();

        RecordId::new(
            PageId::new(u32::from_le_bytes(page_id_bytes)),
            SlotId::new(u16::from_le_bytes(slot_id_bytes)),
        )
    }

    fn set_value_at(&mut self, index: usize, value: RecordId, num_keys: usize) {
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * VALUE_SIZE;

        let page_id_bytes = value.page_id.as_u32().to_le_bytes();
        let slot_id_bytes = value.slot_id.as_u16().to_le_bytes();

        self.data[offset..offset + 4].copy_from_slice(&page_id_bytes);
        self.data[offset + 4..offset + 6].copy_from_slice(&slot_id_bytes);
    }

    pub fn insert_key_child(&mut self, key: u32, child: PageId) -> Result<()> {
        let num_keys = self.num_keys() as usize;
        let pos = self.search_key(key);

        // Read all existing children before modifying anything
        // Internal nodes have num_keys + 1 children
        let mut children: Vec<PageId> = Vec::with_capacity(num_keys + 1);
        for i in 0..=num_keys {
            children.push(self.get_child_at(i, num_keys));
        }

        // Shift keys
        for i in (pos..num_keys).rev() {
            let k = self.get_key(i);
            self.set_key(i + 1, k);
        }

        // Update num_keys
        self.set_num_keys((num_keys + 1) as u16);

        // Set the new key
        self.set_key(pos, key);

        // Write all children to their new positions (all shift due to key array growing)
        for i in 0..=num_keys {
            if i > pos {
                self.set_child_at(i + 1, children[i], num_keys + 1);
            } else {
                self.set_child_at(i, children[i], num_keys + 1);
            }
        }

        // Set the new child
        self.set_child_at(pos + 1, child, num_keys + 1);

        Ok(())
    }

    fn get_child_at(&self, index: usize, num_keys: usize) -> PageId {
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * CHILD_SIZE;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    fn set_child_at(&mut self, index: usize, child: PageId, num_keys: usize) {
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * CHILD_SIZE;
        let bytes = child.as_u32().to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
    }

    pub fn split_leaf(&mut self) -> (u32, Vec<KeyValuePair>) {
        let num_keys = self.num_keys() as usize;
        let mid = num_keys / 2;

        // Read all values before modifying anything
        let mut left_values = Vec::with_capacity(mid);
        for i in 0..mid {
            left_values.push(self.get_value_at(i, num_keys));
        }

        let mut right_pairs = Vec::new();
        for i in mid..num_keys {
            right_pairs.push(KeyValuePair {
                key: self.get_key(i),
                value: self.get_value_at(i, num_keys),
            });
        }

        let separator_key = self.get_key(mid);

        // Update num_keys to mid
        self.set_num_keys(mid as u16);

        // Rewrite left values at their new positions (based on new num_keys)
        for i in 0..mid {
            self.set_value_at(i, left_values[i], mid);
        }

        (separator_key, right_pairs)
    }

    pub fn split_internal(&mut self) -> (u32, Vec<u32>, Vec<PageId>) {
        let num_keys = self.num_keys() as usize;
        let mid = num_keys / 2;

        let separator_key = self.get_key(mid);

        // Read left children before modifying (left side keeps children 0..=mid)
        let mut left_children = Vec::with_capacity(mid + 1);
        for i in 0..=mid {
            left_children.push(self.get_child_at(i, num_keys));
        }

        let mut right_keys = Vec::new();
        let mut right_children = Vec::new();

        for i in (mid + 1)..num_keys {
            right_keys.push(self.get_key(i));
        }

        for i in (mid + 1)..=num_keys {
            right_children.push(self.get_child_at(i, num_keys));
        }

        // Update num_keys to mid
        self.set_num_keys(mid as u16);

        // Rewrite left children at their new positions (based on new num_keys)
        for i in 0..=mid {
            self.set_child_at(i, left_children[i], mid);
        }

        (separator_key, right_keys, right_children)
    }

    pub fn insert_pairs(&mut self, pairs: &[KeyValuePair]) {
        let num_keys = pairs.len();
        self.set_num_keys(num_keys as u16);

        for (i, pair) in pairs.iter().enumerate() {
            self.set_key(i, pair.key);
            self.set_value_at(i, pair.value, num_keys);
        }
    }

    pub fn insert_keys_children(&mut self, keys: &[u32], children: &[PageId]) {
        let num_keys = keys.len();
        self.set_num_keys(num_keys as u16);

        for (i, key) in keys.iter().enumerate() {
            self.set_key(i, *key);
        }
        for (i, child) in children.iter().enumerate() {
            self.set_child_at(i, *child, num_keys);
        }
    }
}

pub struct BTreeNodeRef<'a> {
    data: &'a [u8],
}

impl<'a> BTreeNodeRef<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 4] = self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    pub fn is_leaf(&self) -> bool {
        self.data[IS_LEAF_OFFSET] == 1
    }

    pub fn num_keys(&self) -> u16 {
        let bytes: [u8; 2] = self.data[NUM_KEYS_OFFSET..NUM_KEYS_OFFSET + 2]
            .try_into()
            .unwrap();
        u16::from_le_bytes(bytes)
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.data[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    pub fn get_key(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * KEY_SIZE;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    pub fn get_value(&self, index: usize) -> RecordId {
        let num_keys = self.num_keys() as usize;
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * VALUE_SIZE;

        let page_id_bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        let slot_id_bytes: [u8; 2] = self.data[offset + 4..offset + 6].try_into().unwrap();

        RecordId::new(
            PageId::new(u32::from_le_bytes(page_id_bytes)),
            SlotId::new(u16::from_le_bytes(slot_id_bytes)),
        )
    }

    pub fn get_child(&self, index: usize) -> PageId {
        let num_keys = self.num_keys() as usize;
        let offset = HEADER_SIZE + num_keys * KEY_SIZE + index * CHILD_SIZE;

        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    pub fn search_key(&self, key: u32) -> usize {
        let num_keys = self.num_keys() as usize;
        let mut left = 0;
        let mut right = num_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key(mid);

            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{PageId, RecordId, SlotId, PAGE_SIZE};

    #[test]
    fn test_btree_node_insert_single() {
        let mut data = [0u8; PAGE_SIZE];
        let mut node = BTreeNode::new(&mut data);
        node.init(PageId::new(1), true);

        let record1 = RecordId::new(PageId::new(100), SlotId::new(0));
        node.insert_key_value(10, record1).unwrap();

        assert_eq!(node.num_keys(), 1, "num_keys should be 1");
        assert_eq!(node.get_key(0), 10, "key should be 10");

        let retrieved = node.get_value(0);
        assert_eq!(
            retrieved, record1,
            "Retrieved value doesn't match. Got {:?}, expected {:?}",
            retrieved, record1
        );
    }

    #[test]
    fn test_btree_node_offset_calculation() {
        let mut data = [0u8; PAGE_SIZE];
        let mut node = BTreeNode::new(&mut data);
        node.init(PageId::new(1), true);

        // Manually write a value at the calculated offset
        node.set_num_keys(1);
        let expected_offset = HEADER_SIZE + 1 * KEY_SIZE + 0 * VALUE_SIZE;
        println!(
            "Expected value offset for index 0 with num_keys=1: {}",
            expected_offset
        );

        // Write PageId(100) manually
        let page_id_bytes = 100u32.to_le_bytes();
        node.data[expected_offset..expected_offset + 4].copy_from_slice(&page_id_bytes);

        // Read it back
        let retrieved_offset = HEADER_SIZE + node.num_keys() as usize * KEY_SIZE + 0 * VALUE_SIZE;
        println!("Read offset: {}", retrieved_offset);

        let read_bytes: [u8; 4] = node.data[retrieved_offset..retrieved_offset + 4]
            .try_into()
            .unwrap();
        let read_value = u32::from_le_bytes(read_bytes);

        assert_eq!(read_value, 100, "Value should be 100");
        assert_eq!(expected_offset, retrieved_offset, "Offsets should match");
    }
}
