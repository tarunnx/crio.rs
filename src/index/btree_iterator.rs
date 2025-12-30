use std::sync::Arc;

use crate::buffer::BufferPoolManager;
use crate::common::{CrioError, PageId, RecordId, Result};

use super::btree_page::BTreeNodeRef;

pub struct BTreeIterator {
    bpm: Arc<BufferPoolManager>,
    current_page_id: Option<PageId>,
    current_index: usize,
    end_key: u32,
    done: bool,
}

impl BTreeIterator {
    pub fn new(bpm: Arc<BufferPoolManager>, start_page_id: PageId, end_key: u32) -> Self {
        Self {
            bpm,
            current_page_id: Some(start_page_id),
            current_index: 0,
            end_key,
            done: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<(u32, RecordId)>> {
        if self.done {
            return Ok(None);
        }

        while let Some(page_id) = self.current_page_id {
            let next_page = {
                let guard = self
                    .bpm
                    .checked_read_page(page_id)?
                    .ok_or(CrioError::PageNotFound(page_id))?;
                let node = BTreeNodeRef::new(guard.data());

                if self.current_index < node.num_keys() as usize {
                    let key = node.get_key(self.current_index);

                    if key > self.end_key {
                        self.done = true;
                        return Ok(None);
                    }

                    let value = node.get_value(self.current_index);
                    self.current_index += 1;
                    return Ok(Some((key, value)));
                }

                node.next_page_id()
            };

            self.current_page_id = next_page;
            self.current_index = 0;
        }

        self.done = true;
        Ok(None)
    }
}

impl Iterator for BTreeIterator {
    type Item = Result<(u32, RecordId)>;

    fn next(&mut self) -> Option<Self::Item> {
        match BTreeIterator::next(self) {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
