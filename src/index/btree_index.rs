use std::sync::Arc;

use crate::buffer::BufferPoolManager;
use crate::common::{CrioError, PageId, RecordId, Result, DEFAULT_BTREE_ORDER};

use super::btree_page::{BTreeNode, BTreeNodeRef};

pub struct BTreeIndex {
    root_page_id: PageId,
    bpm: Arc<BufferPoolManager>,
    order: usize,
}

impl BTreeIndex {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Result<Self> {
        let root_page_id = bpm.new_page()?;

        {
            let mut guard = bpm
                .checked_write_page(root_page_id)?
                .ok_or(CrioError::PageNotFound(root_page_id))?;
            let mut node = BTreeNode::new(guard.data_mut());
            node.init(root_page_id, true);
        }

        Ok(Self {
            root_page_id,
            bpm,
            order: DEFAULT_BTREE_ORDER,
        })
    }

    pub fn open(root_page_id: PageId, bpm: Arc<BufferPoolManager>) -> Result<Self> {
        Ok(Self {
            root_page_id,
            bpm,
            order: DEFAULT_BTREE_ORDER,
        })
    }

    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    pub fn search(&self, key: u32) -> Result<Option<RecordId>> {
        let leaf_page_id = self.find_leaf(key)?;

        let guard = self
            .bpm
            .checked_read_page(leaf_page_id)?
            .ok_or(CrioError::PageNotFound(leaf_page_id))?;
        let node = BTreeNodeRef::new(guard.data());

        let pos = node.search_key(key);
        if pos < node.num_keys() as usize && node.get_key(pos) == key {
            Ok(Some(node.get_value(pos)))
        } else {
            Ok(None)
        }
    }

    fn find_leaf(&self, key: u32) -> Result<PageId> {
        let mut current_page_id = self.root_page_id;

        loop {
            let next_page_id = {
                let guard = self
                    .bpm
                    .checked_read_page(current_page_id)?
                    .ok_or(CrioError::PageNotFound(current_page_id))?;
                let node = BTreeNodeRef::new(guard.data());

                if node.is_leaf() {
                    return Ok(current_page_id);
                }

                let pos = node.search_key(key);
                let num_keys = node.num_keys() as usize;

                // For internal nodes: child[i] has keys < keys[i], child[i+1] has keys >= keys[i]
                // search_key returns first index where keys[pos] >= key
                // If key == keys[pos], we need child[pos+1] (keys >= keys[pos])
                let child_index = if pos < num_keys && node.get_key(pos) == key {
                    pos + 1
                } else {
                    pos
                };

                node.get_child(child_index)
            };

            current_page_id = next_page_id;
        }
    }

    pub fn insert(&mut self, key: u32, value: RecordId) -> Result<()> {
        let leaf_page_id = self.find_leaf(key)?;

        let needs_split = {
            let guard = self
                .bpm
                .checked_read_page(leaf_page_id)?
                .ok_or(CrioError::PageNotFound(leaf_page_id))?;
            let node = BTreeNodeRef::new(guard.data());
            node.num_keys() >= self.order as u16
        };

        if needs_split {
            self.split_and_insert_leaf(leaf_page_id, key, value)?;
        } else {
            let mut guard = self
                .bpm
                .checked_write_page(leaf_page_id)?
                .ok_or(CrioError::PageNotFound(leaf_page_id))?;
            let mut node = BTreeNode::new(guard.data_mut());
            node.insert_key_value(key, value)?;
        }

        Ok(())
    }

    fn split_and_insert_leaf(
        &mut self,
        leaf_page_id: PageId,
        key: u32,
        value: RecordId,
    ) -> Result<()> {
        let (separator_key, right_pairs, next_page_id, parent_page_id) = {
            let mut guard = self
                .bpm
                .checked_write_page(leaf_page_id)?
                .ok_or(CrioError::PageNotFound(leaf_page_id))?;
            let mut node = BTreeNode::new(guard.data_mut());

            node.insert_key_value(key, value)?;

            let next = node.next_page_id();
            let parent = node.parent_page_id();
            let (sep_key, pairs) = node.split_leaf();

            (sep_key, pairs, next, parent)
        };

        let new_leaf_id = self.bpm.new_page()?;

        {
            let mut new_guard = self
                .bpm
                .checked_write_page(new_leaf_id)?
                .ok_or(CrioError::PageNotFound(new_leaf_id))?;
            let mut new_node = BTreeNode::new(new_guard.data_mut());
            new_node.init(new_leaf_id, true);
            new_node.insert_pairs(&right_pairs);
            new_node.set_parent_page_id(parent_page_id);
            new_node.set_next_page_id(next_page_id);
            new_node.set_prev_page_id(Some(leaf_page_id));
        }

        {
            let mut guard = self
                .bpm
                .checked_write_page(leaf_page_id)?
                .ok_or(CrioError::PageNotFound(leaf_page_id))?;
            let mut node = BTreeNode::new(guard.data_mut());
            node.set_next_page_id(Some(new_leaf_id));
        }

        if let Some(next_id) = next_page_id {
            let mut next_guard = self
                .bpm
                .checked_write_page(next_id)?
                .ok_or(CrioError::PageNotFound(next_id))?;
            let mut next_node = BTreeNode::new(next_guard.data_mut());
            next_node.set_prev_page_id(Some(new_leaf_id));
        }

        if let Some(parent_id) = parent_page_id {
            self.insert_into_parent(parent_id, separator_key, new_leaf_id)?;
        } else {
            let new_root_id = self.bpm.new_page()?;

            {
                let mut root_guard = self
                    .bpm
                    .checked_write_page(new_root_id)?
                    .ok_or(CrioError::PageNotFound(new_root_id))?;
                let mut root_node = BTreeNode::new(root_guard.data_mut());
                root_node.init(new_root_id, false);
                root_node.insert_keys_children(&[separator_key], &[leaf_page_id, new_leaf_id]);
            }

            {
                let mut guard = self
                    .bpm
                    .checked_write_page(leaf_page_id)?
                    .ok_or(CrioError::PageNotFound(leaf_page_id))?;
                let mut node = BTreeNode::new(guard.data_mut());
                node.set_parent_page_id(Some(new_root_id));
            }

            {
                let mut guard = self
                    .bpm
                    .checked_write_page(new_leaf_id)?
                    .ok_or(CrioError::PageNotFound(new_leaf_id))?;
                let mut node = BTreeNode::new(guard.data_mut());
                node.set_parent_page_id(Some(new_root_id));
            }

            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    fn insert_into_parent(
        &mut self,
        parent_id: PageId,
        key: u32,
        new_child_id: PageId,
    ) -> Result<()> {
        let needs_split = {
            let guard = self
                .bpm
                .checked_read_page(parent_id)?
                .ok_or(CrioError::PageNotFound(parent_id))?;
            let node = BTreeNodeRef::new(guard.data());
            node.num_keys() >= self.order as u16
        };

        if needs_split {
            self.split_and_insert_internal(parent_id, key, new_child_id)?;
        } else {
            let mut guard = self
                .bpm
                .checked_write_page(parent_id)?
                .ok_or(CrioError::PageNotFound(parent_id))?;
            let mut node = BTreeNode::new(guard.data_mut());
            node.insert_key_child(key, new_child_id)?;
        }

        Ok(())
    }

    fn split_and_insert_internal(
        &mut self,
        internal_id: PageId,
        key: u32,
        new_child_id: PageId,
    ) -> Result<()> {
        let (separator_key, right_keys, right_children, parent_page_id) = {
            let mut guard = self
                .bpm
                .checked_write_page(internal_id)?
                .ok_or(CrioError::PageNotFound(internal_id))?;
            let mut node = BTreeNode::new(guard.data_mut());

            node.insert_key_child(key, new_child_id)?;

            let parent = node.parent_page_id();
            let (sep_key, keys, children) = node.split_internal();

            (sep_key, keys, children, parent)
        };

        let new_internal_id = self.bpm.new_page()?;

        {
            let mut new_guard = self
                .bpm
                .checked_write_page(new_internal_id)?
                .ok_or(CrioError::PageNotFound(new_internal_id))?;
            let mut new_node = BTreeNode::new(new_guard.data_mut());
            new_node.init(new_internal_id, false);
            new_node.insert_keys_children(&right_keys, &right_children);
            new_node.set_parent_page_id(parent_page_id);
        }

        for child_id in &right_children {
            let mut child_guard = self
                .bpm
                .checked_write_page(*child_id)?
                .ok_or(CrioError::PageNotFound(*child_id))?;
            let mut child_node = BTreeNode::new(child_guard.data_mut());
            child_node.set_parent_page_id(Some(new_internal_id));
        }

        if let Some(parent_id) = parent_page_id {
            self.insert_into_parent(parent_id, separator_key, new_internal_id)?;
        } else {
            let new_root_id = self.bpm.new_page()?;

            {
                let mut root_guard = self
                    .bpm
                    .checked_write_page(new_root_id)?
                    .ok_or(CrioError::PageNotFound(new_root_id))?;
                let mut root_node = BTreeNode::new(root_guard.data_mut());
                root_node.init(new_root_id, false);
                root_node.insert_keys_children(&[separator_key], &[internal_id, new_internal_id]);
            }

            {
                let mut guard = self
                    .bpm
                    .checked_write_page(internal_id)?
                    .ok_or(CrioError::PageNotFound(internal_id))?;
                let mut node = BTreeNode::new(guard.data_mut());
                node.set_parent_page_id(Some(new_root_id));
            }

            {
                let mut guard = self
                    .bpm
                    .checked_write_page(new_internal_id)?
                    .ok_or(CrioError::PageNotFound(new_internal_id))?;
                let mut node = BTreeNode::new(guard.data_mut());
                node.set_parent_page_id(Some(new_root_id));
            }

            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    pub fn range_scan(&self, start_key: u32, end_key: u32) -> Result<Vec<(u32, RecordId)>> {
        let mut results = Vec::new();
        let leaf_page_id = self.find_leaf(start_key)?;

        let mut current_page_id = Some(leaf_page_id);

        while let Some(page_id) = current_page_id {
            let (next_id, should_continue) = {
                let guard = self
                    .bpm
                    .checked_read_page(page_id)?
                    .ok_or(CrioError::PageNotFound(page_id))?;
                let node = BTreeNodeRef::new(guard.data());

                let num_keys = node.num_keys() as usize;
                let mut found_end = false;

                for i in 0..num_keys {
                    let key = node.get_key(i);
                    if key >= start_key && key <= end_key {
                        results.push((key, node.get_value(i)));
                    }
                    if key > end_key {
                        found_end = true;
                        break;
                    }
                }

                (node.next_page_id(), !found_end)
            };

            if !should_continue {
                break;
            }

            current_page_id = next_id;
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{PageId, RecordId, SlotId};
    use crate::storage::disk::DiskManager;
    use tempfile::NamedTempFile;

    #[test]
    fn test_simple_insert_search() {
        let temp_file = NamedTempFile::new().unwrap();
        let disk_manager = Arc::new(DiskManager::new(temp_file.path()).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(10, 2, disk_manager));

        let mut index = BTreeIndex::new(bpm.clone()).unwrap();
        let record1 = RecordId::new(PageId::new(100), SlotId::new(0));
        let record2 = RecordId::new(PageId::new(100), SlotId::new(1));
        let record3 = RecordId::new(PageId::new(101), SlotId::new(0));

        println!("Root page ID: {:?}", index.root_page_id());

        index.insert(10, record1).unwrap();
        println!("Inserted key=10");

        index.insert(20, record2).unwrap();
        println!("Inserted key=20");

        index.insert(30, record3).unwrap();
        println!("Inserted key=30");

        // Check what's actually stored
        {
            let guard = bpm
                .checked_read_page(index.root_page_id())
                .unwrap()
                .unwrap();
            let node = crate::index::btree_page::BTreeNodeRef::new(guard.data());
            println!("num_keys: {}", node.num_keys());
            for i in 0..node.num_keys() as usize {
                println!(
                    "key[{}]: {}, value[{}]: {:?}",
                    i,
                    node.get_key(i),
                    i,
                    node.get_value(i)
                );
            }
        }

        println!("Searching for key=10");
        let result = index.search(10).unwrap();
        println!("Search result: {:?}", result);
        assert_eq!(result, Some(record1), "Failed to find key 10");

        assert_eq!(
            index.search(20).unwrap(),
            Some(record2),
            "Failed to find key 20"
        );
        assert_eq!(
            index.search(30).unwrap(),
            Some(record3),
            "Failed to find key 30"
        );
    }
}
