use crate::common::{Lsn, PageId, RecordId, Result, SlotId, INVALID_LSN};

use super::slotted_page::{SlottedPage, SlottedPageRef};

/// Table page header layout (after slotted page header):
///
/// | Field              | Offset | Size |
/// |--------------------|--------|------|
/// | next_page_id       | 16     | 4    |
/// | prev_page_id       | 20     | 4    |
/// | lsn                | 24     | 8    |
/// | table_id           | 32     | 4    |
///
/// Total extra header: 20 bytes, total header: 16 + 20 = 36 bytes
const TABLE_HEADER_OFFSET: usize = 16;
const NEXT_PAGE_ID_OFFSET: usize = TABLE_HEADER_OFFSET;
const PREV_PAGE_ID_OFFSET: usize = TABLE_HEADER_OFFSET + 4;
const LSN_OFFSET: usize = TABLE_HEADER_OFFSET + 8;
const TABLE_ID_OFFSET: usize = TABLE_HEADER_OFFSET + 16;

/// Total size of table page header (slotted header + table-specific fields)
const TABLE_HEADER_SIZE: usize = TABLE_ID_OFFSET + 4; // 36 bytes total

/// Invalid page ID for end-of-list markers
const INVALID_PAGE: u32 = u32::MAX;

/// TablePage extends SlottedPage with table-specific metadata and operations.
/// It provides a doubly-linked list structure for table pages.
pub struct TablePage<'a> {
    inner: SlottedPage<'a>,
}

impl<'a> TablePage<'a> {
    /// Creates a new TablePage view over the given data buffer.
    pub fn new(data: &'a mut [u8]) -> Self {
        Self {
            inner: SlottedPage::new(data),
        }
    }

    /// Initializes a fresh table page.
    pub fn init(&mut self, page_id: PageId, table_id: u32) {
        self.inner.init(page_id);
        // Adjust free_space_start to account for the extended table header
        self.inner.set_free_space_start(TABLE_HEADER_SIZE as u16);
        self.set_next_page_id(None);
        self.set_prev_page_id(None);
        self.set_lsn(INVALID_LSN);
        self.set_table_id(table_id);
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.inner.page_id()
    }

    /// Returns the next page ID in the table's page list.
    pub fn next_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.inner.data[NEXT_PAGE_ID_OFFSET..NEXT_PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    /// Sets the next page ID.
    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        let value = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        let bytes = value.to_le_bytes();
        self.inner.data[NEXT_PAGE_ID_OFFSET..NEXT_PAGE_ID_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the previous page ID in the table's page list.
    pub fn prev_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.inner.data[PREV_PAGE_ID_OFFSET..PREV_PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    /// Sets the previous page ID.
    pub fn set_prev_page_id(&mut self, page_id: Option<PageId>) {
        let value = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        let bytes = value.to_le_bytes();
        self.inner.data[PREV_PAGE_ID_OFFSET..PREV_PAGE_ID_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the LSN (Log Sequence Number).
    pub fn lsn(&self) -> Lsn {
        let bytes: [u8; 8] = self.inner.data[LSN_OFFSET..LSN_OFFSET + 8]
            .try_into()
            .unwrap();
        u64::from_le_bytes(bytes)
    }

    /// Sets the LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let bytes = lsn.to_le_bytes();
        self.inner.data[LSN_OFFSET..LSN_OFFSET + 8].copy_from_slice(&bytes);
    }

    /// Returns the table ID.
    pub fn table_id(&self) -> u32 {
        let bytes: [u8; 4] = self.inner.data[TABLE_ID_OFFSET..TABLE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes)
    }

    /// Sets the table ID.
    pub fn set_table_id(&mut self, table_id: u32) {
        let bytes = table_id.to_le_bytes();
        self.inner.data[TABLE_ID_OFFSET..TABLE_ID_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Inserts a tuple and returns its record ID.
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<RecordId> {
        let slot_id = self.inner.insert_tuple(tuple)?;
        Ok(RecordId::new(self.page_id(), slot_id))
    }

    /// Gets a tuple by slot ID.
    pub fn get_tuple(&self, slot_id: SlotId) -> Result<&[u8]> {
        self.inner.get_tuple(slot_id)
    }

    /// Gets a mutable reference to a tuple.
    pub fn get_tuple_mut(&mut self, slot_id: SlotId) -> Result<&mut [u8]> {
        self.inner.get_tuple_mut(slot_id)
    }

    /// Deletes a tuple by slot ID.
    pub fn delete_tuple(&mut self, slot_id: SlotId) -> Result<()> {
        self.inner.delete_tuple(slot_id)
    }

    /// Updates a tuple in place.
    pub fn update_tuple(&mut self, slot_id: SlotId, new_data: &[u8]) -> Result<()> {
        self.inner.update_tuple(slot_id, new_data)
    }

    /// Returns whether there's enough space to insert a tuple.
    pub fn can_insert(&self, tuple_size: usize) -> bool {
        self.inner.can_insert(tuple_size)
    }

    /// Returns the amount of free space.
    pub fn free_space(&self) -> usize {
        self.inner.free_space()
    }

    /// Returns the number of non-empty tuples.
    pub fn tuple_count(&self) -> usize {
        self.inner.tuple_count()
    }

    /// Returns an iterator over all record IDs in this page.
    pub fn record_ids(&self) -> impl Iterator<Item = RecordId> + '_ {
        let page_id = self.page_id();
        self.inner
            .slot_ids()
            .map(move |slot_id| RecordId::new(page_id, slot_id))
    }

    /// Compacts the page, reclaiming space from deleted tuples.
    pub fn compact(&mut self) {
        self.inner.compact()
    }
}

/// Read-only view of a table page.
pub struct TablePageRef<'a> {
    inner: SlottedPageRef<'a>,
}

impl<'a> TablePageRef<'a> {
    /// Creates a new read-only TablePage view.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            inner: SlottedPageRef::new(data),
        }
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.inner.page_id()
    }

    /// Returns the next page ID.
    pub fn next_page_id(&self) -> Option<PageId> {
        let bytes: [u8; 4] = self.inner.data[NEXT_PAGE_ID_OFFSET..NEXT_PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        let value = u32::from_le_bytes(bytes);
        if value == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(value))
        }
    }

    /// Returns the table ID.
    pub fn table_id(&self) -> u32 {
        let bytes: [u8; 4] = self.inner.data[TABLE_ID_OFFSET..TABLE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes)
    }

    /// Gets a tuple by slot ID.
    pub fn get_tuple(&self, slot_id: SlotId) -> Result<&[u8]> {
        self.inner.get_tuple(slot_id)
    }

    /// Returns the number of non-empty tuples.
    pub fn tuple_count(&self) -> usize {
        self.inner.tuple_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PAGE_SIZE;

    #[test]
    fn test_table_page_init() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = TablePage::new(&mut data);
        page.init(PageId::new(1), 42);

        assert_eq!(page.page_id(), PageId::new(1));
        assert_eq!(page.table_id(), 42);
        assert_eq!(page.next_page_id(), None);
        assert_eq!(page.prev_page_id(), None);
        assert_eq!(page.lsn(), INVALID_LSN);
    }

    #[test]
    fn test_table_page_links() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = TablePage::new(&mut data);
        page.init(PageId::new(1), 42);

        page.set_next_page_id(Some(PageId::new(2)));
        page.set_prev_page_id(Some(PageId::new(0)));

        assert_eq!(page.next_page_id(), Some(PageId::new(2)));
        assert_eq!(page.prev_page_id(), Some(PageId::new(0)));
    }

    #[test]
    fn test_table_page_insert() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = TablePage::new(&mut data);
        page.init(PageId::new(1), 42);

        let tuple = b"Hello, World!";
        let rid = page.insert_tuple(tuple).unwrap();

        assert_eq!(rid.page_id, PageId::new(1));
        assert_eq!(rid.slot_id, SlotId::new(0));
        assert_eq!(page.get_tuple(rid.slot_id).unwrap(), tuple);
    }

    #[test]
    fn test_table_page_record_ids() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = TablePage::new(&mut data);
        page.init(PageId::new(1), 42);

        page.insert_tuple(b"First").unwrap();
        page.insert_tuple(b"Second").unwrap();
        page.insert_tuple(b"Third").unwrap();

        let rids: Vec<_> = page.record_ids().collect();
        assert_eq!(rids.len(), 3);
        assert_eq!(rids[0].page_id, PageId::new(1));
        assert_eq!(rids[0].slot_id, SlotId::new(0));
    }

    #[test]
    fn test_table_page_ref() {
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut page = TablePage::new(&mut data);
            page.init(PageId::new(1), 42);
            page.set_next_page_id(Some(PageId::new(2)));
            page.insert_tuple(b"Test").unwrap();
        }

        let page_ref = TablePageRef::new(&data);
        assert_eq!(page_ref.page_id(), PageId::new(1));
        assert_eq!(page_ref.table_id(), 42);
        assert_eq!(page_ref.next_page_id(), Some(PageId::new(2)));
        assert_eq!(page_ref.tuple_count(), 1);
    }
}
