use crate::common::{CrioError, PageId, Result, PAGE_SIZE};

const MAGIC_NUMBER: u32 = 0x4352494F; // "CRIO" in hex
const VERSION: u32 = 1;

const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 4;
const PAGE_COUNT_OFFSET: usize = 8;
const FREE_PAGE_LIST_HEAD_OFFSET: usize = 12;
const TABLE_COUNT_OFFSET: usize = 16;
const TABLE_ENTRIES_OFFSET: usize = 20;

const TABLE_ENTRY_SIZE: usize = 12; // table_id (4) + first_page (4) + page_count (4)
const MAX_TABLES: usize = (PAGE_SIZE - TABLE_ENTRIES_OFFSET) / TABLE_ENTRY_SIZE;

const INVALID_PAGE: u32 = u32::MAX;

#[derive(Debug, Clone, Copy)]
pub struct TableEntry {
    pub table_id: u32,
    pub first_page_id: PageId,
    pub page_count: u32,
}

pub struct DirectoryPage<'a> {
    data: &'a mut [u8],
}

impl<'a> DirectoryPage<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    pub fn init(&mut self) {
        self.data.fill(0);
        self.set_magic(MAGIC_NUMBER);
        self.set_version(VERSION);
        self.set_page_count(1);
        self.set_free_page_list_head(None);
        self.set_table_count(0);
    }

    pub fn is_valid(&self) -> bool {
        self.magic() == MAGIC_NUMBER
    }

    pub fn magic(&self) -> u32 {
        u32::from_le_bytes(
            self.data[MAGIC_OFFSET..MAGIC_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn set_magic(&mut self, magic: u32) {
        self.data[MAGIC_OFFSET..MAGIC_OFFSET + 4].copy_from_slice(&magic.to_le_bytes());
    }

    pub fn version(&self) -> u32 {
        u32::from_le_bytes(
            self.data[VERSION_OFFSET..VERSION_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn set_version(&mut self, version: u32) {
        self.data[VERSION_OFFSET..VERSION_OFFSET + 4].copy_from_slice(&version.to_le_bytes());
    }

    pub fn page_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn set_page_count(&mut self, count: u32) {
        self.data[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 4].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_page_list_head(&self) -> Option<PageId> {
        let val = u32::from_le_bytes(
            self.data[FREE_PAGE_LIST_HEAD_OFFSET..FREE_PAGE_LIST_HEAD_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        if val == INVALID_PAGE {
            None
        } else {
            Some(PageId::new(val))
        }
    }

    pub fn set_free_page_list_head(&mut self, page_id: Option<PageId>) {
        let val = page_id.map(|p| p.as_u32()).unwrap_or(INVALID_PAGE);
        self.data[FREE_PAGE_LIST_HEAD_OFFSET..FREE_PAGE_LIST_HEAD_OFFSET + 4]
            .copy_from_slice(&val.to_le_bytes());
    }

    pub fn table_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[TABLE_COUNT_OFFSET..TABLE_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn set_table_count(&mut self, count: u32) {
        self.data[TABLE_COUNT_OFFSET..TABLE_COUNT_OFFSET + 4].copy_from_slice(&count.to_le_bytes());
    }

    fn table_entry_offset(index: usize) -> usize {
        TABLE_ENTRIES_OFFSET + index * TABLE_ENTRY_SIZE
    }

    pub fn get_table_entry(&self, index: usize) -> Option<TableEntry> {
        if index >= self.table_count() as usize {
            return None;
        }

        let offset = Self::table_entry_offset(index);
        let table_id = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap());
        let first_page = u32::from_le_bytes(self.data[offset + 4..offset + 8].try_into().unwrap());
        let page_count = u32::from_le_bytes(self.data[offset + 8..offset + 12].try_into().unwrap());

        Some(TableEntry {
            table_id,
            first_page_id: PageId::new(first_page),
            page_count,
        })
    }

    fn set_table_entry(&mut self, index: usize, entry: &TableEntry) {
        let offset = Self::table_entry_offset(index);
        self.data[offset..offset + 4].copy_from_slice(&entry.table_id.to_le_bytes());
        self.data[offset + 4..offset + 8]
            .copy_from_slice(&entry.first_page_id.as_u32().to_le_bytes());
        self.data[offset + 8..offset + 12].copy_from_slice(&entry.page_count.to_le_bytes());
    }

    pub fn find_table(&self, table_id: u32) -> Option<TableEntry> {
        for i in 0..self.table_count() as usize {
            if let Some(entry) = self.get_table_entry(i) {
                if entry.table_id == table_id {
                    return Some(entry);
                }
            }
        }
        None
    }

    pub fn register_table(&mut self, table_id: u32, first_page_id: PageId) -> Result<()> {
        if self.find_table(table_id).is_some() {
            return Err(CrioError::TableAlreadyExists(table_id));
        }

        let count = self.table_count() as usize;
        if count >= MAX_TABLES {
            return Err(CrioError::DirectoryFull);
        }

        let entry = TableEntry {
            table_id,
            first_page_id,
            page_count: 1,
        };

        self.set_table_entry(count, &entry);
        self.set_table_count((count + 1) as u32);

        Ok(())
    }

    pub fn update_table_page_count(&mut self, table_id: u32, page_count: u32) -> Result<()> {
        for i in 0..self.table_count() as usize {
            if let Some(mut entry) = self.get_table_entry(i) {
                if entry.table_id == table_id {
                    entry.page_count = page_count;
                    self.set_table_entry(i, &entry);
                    return Ok(());
                }
            }
        }
        Err(CrioError::TableNotFound(table_id))
    }

    pub fn remove_table(&mut self, table_id: u32) -> Result<TableEntry> {
        let count = self.table_count() as usize;

        for i in 0..count {
            if let Some(entry) = self.get_table_entry(i) {
                if entry.table_id == table_id {
                    if i < count - 1 {
                        if let Some(last_entry) = self.get_table_entry(count - 1) {
                            self.set_table_entry(i, &last_entry);
                        }
                    }
                    self.set_table_count((count - 1) as u32);
                    return Ok(entry);
                }
            }
        }

        Err(CrioError::TableNotFound(table_id))
    }

    pub fn all_tables(&self) -> Vec<TableEntry> {
        let mut tables = Vec::new();
        for i in 0..self.table_count() as usize {
            if let Some(entry) = self.get_table_entry(i) {
                tables.push(entry);
            }
        }
        tables
    }

    pub fn increment_page_count(&mut self) -> u32 {
        let count = self.page_count() + 1;
        self.set_page_count(count);
        count
    }
}

pub struct DirectoryPageRef<'a> {
    data: &'a [u8],
}

impl<'a> DirectoryPageRef<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    pub fn is_valid(&self) -> bool {
        self.magic() == MAGIC_NUMBER
    }

    pub fn magic(&self) -> u32 {
        u32::from_le_bytes(
            self.data[MAGIC_OFFSET..MAGIC_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn version(&self) -> u32 {
        u32::from_le_bytes(
            self.data[VERSION_OFFSET..VERSION_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn page_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn table_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[TABLE_COUNT_OFFSET..TABLE_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn find_table(&self, table_id: u32) -> Option<TableEntry> {
        for i in 0..self.table_count() as usize {
            let offset = TABLE_ENTRIES_OFFSET + i * TABLE_ENTRY_SIZE;
            let tid = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap());
            if tid == table_id {
                let first_page =
                    u32::from_le_bytes(self.data[offset + 4..offset + 8].try_into().unwrap());
                let page_count =
                    u32::from_le_bytes(self.data[offset + 8..offset + 12].try_into().unwrap());
                return Some(TableEntry {
                    table_id,
                    first_page_id: PageId::new(first_page),
                    page_count,
                });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_page_init() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = DirectoryPage::new(&mut data);
        page.init();

        assert!(page.is_valid());
        assert_eq!(page.version(), VERSION);
        assert_eq!(page.page_count(), 1);
        assert_eq!(page.free_page_list_head(), None);
        assert_eq!(page.table_count(), 0);
    }

    #[test]
    fn test_directory_page_register_table() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = DirectoryPage::new(&mut data);
        page.init();

        page.register_table(1, PageId::new(1)).unwrap();
        page.register_table(2, PageId::new(9)).unwrap();

        assert_eq!(page.table_count(), 2);

        let entry1 = page.find_table(1).unwrap();
        assert_eq!(entry1.table_id, 1);
        assert_eq!(entry1.first_page_id, PageId::new(1));

        let entry2 = page.find_table(2).unwrap();
        assert_eq!(entry2.table_id, 2);
        assert_eq!(entry2.first_page_id, PageId::new(9));
    }

    #[test]
    fn test_directory_page_duplicate_table() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = DirectoryPage::new(&mut data);
        page.init();

        page.register_table(1, PageId::new(1)).unwrap();
        let result = page.register_table(1, PageId::new(2));

        assert!(matches!(result, Err(CrioError::TableAlreadyExists(1))));
    }

    #[test]
    fn test_directory_page_remove_table() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = DirectoryPage::new(&mut data);
        page.init();

        page.register_table(1, PageId::new(1)).unwrap();
        page.register_table(2, PageId::new(9)).unwrap();
        page.register_table(3, PageId::new(17)).unwrap();

        let removed = page.remove_table(2).unwrap();
        assert_eq!(removed.table_id, 2);
        assert_eq!(page.table_count(), 2);

        assert!(page.find_table(1).is_some());
        assert!(page.find_table(2).is_none());
        assert!(page.find_table(3).is_some());
    }

    #[test]
    fn test_directory_page_update_page_count() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = DirectoryPage::new(&mut data);
        page.init();

        page.register_table(1, PageId::new(1)).unwrap();
        page.update_table_page_count(1, 5).unwrap();

        let entry = page.find_table(1).unwrap();
        assert_eq!(entry.page_count, 5);
    }

    #[test]
    fn test_directory_page_ref() {
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut page = DirectoryPage::new(&mut data);
            page.init();
            page.register_table(1, PageId::new(1)).unwrap();
        }

        let page_ref = DirectoryPageRef::new(&data);
        assert!(page_ref.is_valid());
        assert_eq!(page_ref.table_count(), 1);

        let entry = page_ref.find_table(1).unwrap();
        assert_eq!(entry.first_page_id, PageId::new(1));
    }
}
