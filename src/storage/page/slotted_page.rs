use crate::common::{CrioError, PageId, Result, SlotId, PAGE_SIZE};

/// Slotted page layout:
///
/// +------------------+
/// | Page Header      |  (HEADER_SIZE bytes)
/// +------------------+
/// | Slot Array       |  (grows downward)
/// | [slot 0]         |
/// | [slot 1]         |
/// | ...              |
/// +------------------+
/// |                  |
/// | Free Space       |
/// |                  |
/// +------------------+
/// | Tuple Data       |  (grows upward from bottom)
/// | [tuple n]        |
/// | [tuple n-1]      |
/// | ...              |
/// +------------------+
///
/// Each slot entry contains:
///   - offset: u16 (offset from start of page to tuple data)
///   - length: u16 (length of the tuple)
///   - A length of 0 indicates an empty/deleted slot
const HEADER_SIZE: usize = 16;

/// Size of each slot entry in bytes
const SLOT_SIZE: usize = 4;

/// Offset of page_id field in header
const PAGE_ID_OFFSET: usize = 0;

/// Offset of num_slots field in header
const NUM_SLOTS_OFFSET: usize = 4;

/// Offset of free_space_start field in header
const FREE_SPACE_START_OFFSET: usize = 8;

/// Offset of free_space_end field in header
const FREE_SPACE_END_OFFSET: usize = 12;

/// Represents a slot entry in the slot array
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    /// Offset from start of page to tuple data
    pub offset: u16,
    /// Length of the tuple (0 = empty/deleted)
    pub length: u16,
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self {
        Self { offset, length }
    }

    pub fn empty() -> Self {
        Self {
            offset: 0,
            length: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

/// SlottedPage provides methods to interpret and manipulate a page
/// as a slotted page with variable-length tuples.
pub struct SlottedPage<'a> {
    pub(crate) data: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// Creates a new SlottedPage view over the given data buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    /// Initializes a fresh slotted page with the given page ID.
    pub fn init(&mut self, page_id: PageId) {
        self.data.fill(0);
        self.set_page_id(page_id);
        self.set_num_slots(0);
        self.set_free_space_start(HEADER_SIZE as u16);
        self.set_free_space_end(PAGE_SIZE as u16);
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 4] = self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    /// Sets the page ID.
    pub fn set_page_id(&mut self, page_id: PageId) {
        let bytes = page_id.as_u32().to_le_bytes();
        self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the number of slots.
    pub fn num_slots(&self) -> u16 {
        let bytes: [u8; 4] = self.data[NUM_SLOTS_OFFSET..NUM_SLOTS_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes) as u16
    }

    /// Sets the number of slots.
    fn set_num_slots(&mut self, num_slots: u16) {
        let bytes = (num_slots as u32).to_le_bytes();
        self.data[NUM_SLOTS_OFFSET..NUM_SLOTS_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the start of free space (end of slot array).
    pub fn free_space_start(&self) -> u16 {
        let bytes: [u8; 4] = self.data[FREE_SPACE_START_OFFSET..FREE_SPACE_START_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes) as u16
    }

    /// Sets the start of free space.
    pub(crate) fn set_free_space_start(&mut self, offset: u16) {
        let bytes = (offset as u32).to_le_bytes();
        self.data[FREE_SPACE_START_OFFSET..FREE_SPACE_START_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the end of free space (start of tuple data area).
    pub fn free_space_end(&self) -> u16 {
        let bytes: [u8; 4] = self.data[FREE_SPACE_END_OFFSET..FREE_SPACE_END_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes) as u16
    }

    /// Sets the end of free space.
    fn set_free_space_end(&mut self, offset: u16) {
        let bytes = (offset as u32).to_le_bytes();
        self.data[FREE_SPACE_END_OFFSET..FREE_SPACE_END_OFFSET + 4].copy_from_slice(&bytes);
    }

    /// Returns the amount of free space available.
    pub fn free_space(&self) -> usize {
        let start = self.free_space_start() as usize;
        let end = self.free_space_end() as usize;
        end.saturating_sub(start)
    }

    /// Returns whether there's enough space to insert a tuple of the given size.
    pub fn can_insert(&self, tuple_size: usize) -> bool {
        // Need space for the tuple data plus a new slot entry
        self.free_space() >= tuple_size + SLOT_SIZE
    }

    /// Computes the base offset where slot array starts.
    /// This is derived from free_space_start and the number of slots.
    fn slot_array_base(&self) -> usize {
        let num_slots = self.num_slots() as usize;
        (self.free_space_start() as usize).saturating_sub(num_slots * SLOT_SIZE)
    }

    /// Gets a slot entry by slot ID.
    pub fn get_slot(&self, slot_id: SlotId) -> Option<SlotEntry> {
        let slot_num = slot_id.as_u16();
        if slot_num >= self.num_slots() {
            return None;
        }

        let slot_offset = self.slot_array_base() + (slot_num as usize) * SLOT_SIZE;
        let offset_bytes: [u8; 2] = self.data[slot_offset..slot_offset + 2].try_into().unwrap();
        let length_bytes: [u8; 2] = self.data[slot_offset + 2..slot_offset + 4]
            .try_into()
            .unwrap();

        Some(SlotEntry::new(
            u16::from_le_bytes(offset_bytes),
            u16::from_le_bytes(length_bytes),
        ))
    }

    /// Sets a slot entry.
    fn set_slot(&mut self, slot_id: SlotId, entry: SlotEntry) {
        let slot_num = slot_id.as_u16();
        let slot_offset = self.slot_array_base() + (slot_num as usize) * SLOT_SIZE;

        let offset_bytes = entry.offset.to_le_bytes();
        let length_bytes = entry.length.to_le_bytes();

        self.data[slot_offset..slot_offset + 2].copy_from_slice(&offset_bytes);
        self.data[slot_offset + 2..slot_offset + 4].copy_from_slice(&length_bytes);
    }

    /// Inserts a tuple and returns its slot ID.
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<SlotId> {
        let tuple_size = tuple.len();

        if !self.can_insert(tuple_size) {
            return Err(CrioError::PageOverflow {
                tuple_size,
                available: self.free_space().saturating_sub(SLOT_SIZE),
            });
        }

        // Find an empty slot or create a new one
        let (slot_id, is_new_slot) = self.find_or_create_slot();

        // Update free_space_start BEFORE set_slot so slot_array_base is correct
        if is_new_slot {
            self.set_free_space_start(self.free_space_start() + SLOT_SIZE as u16);
        }

        // Calculate tuple position (grow from end of page)
        let tuple_offset = self.free_space_end() - tuple_size as u16;

        // Write the tuple
        self.data[tuple_offset as usize..(tuple_offset as usize + tuple_size)]
            .copy_from_slice(tuple);

        // Update slot entry
        self.set_slot(slot_id, SlotEntry::new(tuple_offset, tuple_size as u16));

        // Update free space end
        self.set_free_space_end(tuple_offset);

        Ok(slot_id)
    }

    /// Finds an empty slot or creates a new one.
    fn find_or_create_slot(&mut self) -> (SlotId, bool) {
        let num_slots = self.num_slots();

        // Look for an empty slot
        for i in 0..num_slots {
            if let Some(entry) = self.get_slot(SlotId::new(i)) {
                if entry.is_empty() {
                    return (SlotId::new(i), false);
                }
            }
        }

        // Create a new slot
        self.set_num_slots(num_slots + 1);
        (SlotId::new(num_slots), true)
    }

    /// Gets tuple data by slot ID.
    pub fn get_tuple(&self, slot_id: SlotId) -> Result<&[u8]> {
        let entry = self
            .get_slot(slot_id)
            .ok_or(CrioError::InvalidSlotId(slot_id.as_u16()))?;

        if entry.is_empty() {
            return Err(CrioError::EmptySlot(slot_id.as_u16()));
        }

        let start = entry.offset as usize;
        let end = start + entry.length as usize;

        Ok(&self.data[start..end])
    }

    /// Gets mutable tuple data by slot ID.
    pub fn get_tuple_mut(&mut self, slot_id: SlotId) -> Result<&mut [u8]> {
        let entry = self
            .get_slot(slot_id)
            .ok_or(CrioError::InvalidSlotId(slot_id.as_u16()))?;

        if entry.is_empty() {
            return Err(CrioError::EmptySlot(slot_id.as_u16()));
        }

        let start = entry.offset as usize;
        let end = start + entry.length as usize;

        Ok(&mut self.data[start..end])
    }

    /// Deletes a tuple by slot ID.
    /// This marks the slot as empty but doesn't reclaim the space.
    pub fn delete_tuple(&mut self, slot_id: SlotId) -> Result<()> {
        if self.get_slot(slot_id).is_none() {
            return Err(CrioError::InvalidSlotId(slot_id.as_u16()));
        }

        // Mark slot as empty
        self.set_slot(slot_id, SlotEntry::empty());

        Ok(())
    }

    /// Updates a tuple in place. The new data must fit in the existing slot.
    pub fn update_tuple(&mut self, slot_id: SlotId, new_data: &[u8]) -> Result<()> {
        let entry = self
            .get_slot(slot_id)
            .ok_or(CrioError::InvalidSlotId(slot_id.as_u16()))?;

        if entry.is_empty() {
            return Err(CrioError::EmptySlot(slot_id.as_u16()));
        }

        if new_data.len() > entry.length as usize {
            return Err(CrioError::PageOverflow {
                tuple_size: new_data.len(),
                available: entry.length as usize,
            });
        }

        let start = entry.offset as usize;
        self.data[start..start + new_data.len()].copy_from_slice(new_data);

        // Update slot length if smaller
        if new_data.len() < entry.length as usize {
            self.set_slot(slot_id, SlotEntry::new(entry.offset, new_data.len() as u16));
        }

        Ok(())
    }

    /// Compacts the page, reclaiming space from deleted tuples.
    /// This is an expensive operation and should be done sparingly.
    pub fn compact(&mut self) {
        let num_slots = self.num_slots();
        if num_slots == 0 {
            return;
        }

        // Collect non-empty tuples with their slot IDs
        let mut tuples: Vec<(SlotId, Vec<u8>)> = Vec::new();
        for i in 0..num_slots {
            let slot_id = SlotId::new(i);
            if let Ok(tuple) = self.get_tuple(slot_id) {
                tuples.push((slot_id, tuple.to_vec()));
            }
        }

        // Reset the data area
        self.set_free_space_end(PAGE_SIZE as u16);

        // Clear all slots
        for i in 0..num_slots {
            self.set_slot(SlotId::new(i), SlotEntry::empty());
        }

        // Reinsert all tuples in order
        for (slot_id, tuple) in tuples {
            let tuple_offset = self.free_space_end() - tuple.len() as u16;

            self.data[tuple_offset as usize..tuple_offset as usize + tuple.len()]
                .copy_from_slice(&tuple);

            self.set_slot(slot_id, SlotEntry::new(tuple_offset, tuple.len() as u16));

            self.set_free_space_end(tuple_offset);
        }
    }

    /// Returns an iterator over all non-empty slot IDs.
    pub fn slot_ids(&self) -> impl Iterator<Item = SlotId> + '_ {
        let num_slots = self.num_slots();
        (0..num_slots).filter_map(move |i| {
            let slot_id = SlotId::new(i);
            self.get_slot(slot_id)
                .filter(|e| !e.is_empty())
                .map(|_| slot_id)
        })
    }

    /// Returns the number of non-empty tuples.
    pub fn tuple_count(&self) -> usize {
        self.slot_ids().count()
    }
}

/// Read-only view of a slotted page.
pub struct SlottedPageRef<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> SlottedPageRef<'a> {
    /// Creates a new read-only SlottedPage view.
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(data.len(), PAGE_SIZE);
        Self { data }
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 4] = self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4]
            .try_into()
            .unwrap();
        PageId::new(u32::from_le_bytes(bytes))
    }

    /// Returns the number of slots.
    pub fn num_slots(&self) -> u16 {
        let bytes: [u8; 4] = self.data[NUM_SLOTS_OFFSET..NUM_SLOTS_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes) as u16
    }

    /// Returns the amount of free space.
    pub fn free_space(&self) -> usize {
        let start = {
            let bytes: [u8; 4] = self.data[FREE_SPACE_START_OFFSET..FREE_SPACE_START_OFFSET + 4]
                .try_into()
                .unwrap();
            u32::from_le_bytes(bytes) as usize
        };
        let end = {
            let bytes: [u8; 4] = self.data[FREE_SPACE_END_OFFSET..FREE_SPACE_END_OFFSET + 4]
                .try_into()
                .unwrap();
            u32::from_le_bytes(bytes) as usize
        };
        end.saturating_sub(start)
    }

    /// Returns the start of free space.
    fn free_space_start(&self) -> u16 {
        let bytes: [u8; 4] = self.data[FREE_SPACE_START_OFFSET..FREE_SPACE_START_OFFSET + 4]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes) as u16
    }

    /// Computes the base offset where slot array starts.
    fn slot_array_base(&self) -> usize {
        let num_slots = self.num_slots() as usize;
        (self.free_space_start() as usize).saturating_sub(num_slots * SLOT_SIZE)
    }

    /// Gets a slot entry by slot ID.
    pub fn get_slot(&self, slot_id: SlotId) -> Option<SlotEntry> {
        let slot_num = slot_id.as_u16();
        if slot_num >= self.num_slots() {
            return None;
        }

        let slot_offset = self.slot_array_base() + (slot_num as usize) * SLOT_SIZE;
        let offset_bytes: [u8; 2] = self.data[slot_offset..slot_offset + 2].try_into().unwrap();
        let length_bytes: [u8; 2] = self.data[slot_offset + 2..slot_offset + 4]
            .try_into()
            .unwrap();

        Some(SlotEntry::new(
            u16::from_le_bytes(offset_bytes),
            u16::from_le_bytes(length_bytes),
        ))
    }

    /// Gets tuple data by slot ID.
    pub fn get_tuple(&self, slot_id: SlotId) -> Result<&[u8]> {
        let entry = self
            .get_slot(slot_id)
            .ok_or(CrioError::InvalidSlotId(slot_id.as_u16()))?;

        if entry.is_empty() {
            return Err(CrioError::EmptySlot(slot_id.as_u16()));
        }

        let start = entry.offset as usize;
        let end = start + entry.length as usize;

        Ok(&self.data[start..end])
    }

    /// Returns the number of non-empty tuples.
    pub fn tuple_count(&self) -> usize {
        let num_slots = self.num_slots();
        (0..num_slots)
            .filter(|&i| {
                self.get_slot(SlotId::new(i))
                    .map(|e| !e.is_empty())
                    .unwrap_or(false)
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slotted_page_init() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        assert_eq!(page.page_id(), PageId::new(1));
        assert_eq!(page.num_slots(), 0);
        assert_eq!(page.free_space_start(), HEADER_SIZE as u16);
        assert_eq!(page.free_space_end(), PAGE_SIZE as u16);
    }

    #[test]
    fn test_slotted_page_insert() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let tuple = b"Hello, World!";
        let slot_id = page.insert_tuple(tuple).unwrap();

        assert_eq!(slot_id, SlotId::new(0));
        assert_eq!(page.num_slots(), 1);
        assert_eq!(page.get_tuple(slot_id).unwrap(), tuple);
    }

    #[test]
    fn test_slotted_page_multiple_inserts() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let tuples = [b"First".as_slice(), b"Second", b"Third"];
        let mut slot_ids = Vec::new();

        for tuple in &tuples {
            slot_ids.push(page.insert_tuple(tuple).unwrap());
        }

        assert_eq!(page.num_slots(), 3);
        assert_eq!(page.tuple_count(), 3);

        for (i, tuple) in tuples.iter().enumerate() {
            assert_eq!(page.get_tuple(slot_ids[i]).unwrap(), *tuple);
        }
    }

    #[test]
    fn test_slotted_page_delete() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let slot_id = page.insert_tuple(b"Test").unwrap();
        assert_eq!(page.tuple_count(), 1);

        page.delete_tuple(slot_id).unwrap();
        assert_eq!(page.tuple_count(), 0);
        assert!(page.get_tuple(slot_id).is_err());
    }

    #[test]
    fn test_slotted_page_reuse_slot() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let slot_id1 = page.insert_tuple(b"First").unwrap();
        let _slot_id2 = page.insert_tuple(b"Second").unwrap();

        page.delete_tuple(slot_id1).unwrap();

        // Insert should reuse the deleted slot
        let slot_id3 = page.insert_tuple(b"Third").unwrap();
        assert_eq!(slot_id3, slot_id1);
    }

    #[test]
    fn test_slotted_page_update() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let slot_id = page.insert_tuple(b"Hello").unwrap();
        page.update_tuple(slot_id, b"Hi").unwrap();

        assert_eq!(page.get_tuple(slot_id).unwrap(), b"Hi");
    }

    #[test]
    fn test_slotted_page_update_too_large() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let slot_id = page.insert_tuple(b"Hi").unwrap();
        let result = page.update_tuple(slot_id, b"Hello, World!");

        assert!(result.is_err());
    }

    #[test]
    fn test_slotted_page_full() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        // Insert tuples until full
        let large_tuple = [0u8; 1000];
        let mut count = 0;

        while page.can_insert(large_tuple.len()) {
            page.insert_tuple(&large_tuple).unwrap();
            count += 1;
        }

        assert!(count > 0);
        assert!(!page.can_insert(large_tuple.len()));
        assert!(page.insert_tuple(&large_tuple).is_err());
    }

    #[test]
    fn test_slotted_page_compact() {
        let mut data = [0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);
        page.init(PageId::new(1));

        let slot_id1 = page.insert_tuple(b"First").unwrap();
        let slot_id2 = page.insert_tuple(b"Second").unwrap();
        let slot_id3 = page.insert_tuple(b"Third").unwrap();

        let free_before = page.free_space();

        // Delete middle tuple
        page.delete_tuple(slot_id2).unwrap();

        // Free space shouldn't change yet
        assert_eq!(page.free_space(), free_before);

        // Compact
        page.compact();

        // Free space should increase
        assert!(page.free_space() > free_before);

        // Remaining tuples should still be accessible
        assert_eq!(page.get_tuple(slot_id1).unwrap(), b"First");
        assert_eq!(page.get_tuple(slot_id3).unwrap(), b"Third");
        assert!(page.get_tuple(slot_id2).is_err());
    }

    #[test]
    fn test_slotted_page_ref() {
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut page = SlottedPage::new(&mut data);
            page.init(PageId::new(1));
            page.insert_tuple(b"Test").unwrap();
        }

        let page_ref = SlottedPageRef::new(&data);
        assert_eq!(page_ref.page_id(), PageId::new(1));
        assert_eq!(page_ref.tuple_count(), 1);
        assert_eq!(page_ref.get_tuple(SlotId::new(0)).unwrap(), b"Test");
    }
}
