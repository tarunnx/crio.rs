use std::fmt;

/// Page identifier type - uniquely identifies a page on disk.
/// Uses bit-packing to support multi-file addressing:
/// - High 8 bits: File ID (up to 256 files)
/// - Low 24 bits: Page Offset (up to 16M pages per file)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u32);

impl PageId {
    /// Mask for extracting the Page Offset (lower 24 bits)
    pub const PAGE_OFFSET_MASK: u32 = 0x00FF_FFFF;
    /// Mask for extracting the File ID (upper 8 bits)
    pub const FILE_ID_MASK: u32 = 0xFF00_0000;
    /// Number of bits to shift for File ID
    pub const FILE_ID_SHIFT: u32 = 24;

    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Creates a PageId from a specific File ID and Page Offset
    pub fn from_parts(file_id: u8, page_offset: u32) -> Self {
        // Ensure page_offset fits in 24 bits
        assert!(page_offset <= Self::PAGE_OFFSET_MASK, "Page offset too large");
        let id = ((file_id as u32) << Self::FILE_ID_SHIFT) | (page_offset & Self::PAGE_OFFSET_MASK);
        Self(id)
    }

    pub const fn new_const(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    /// Returns the File ID component of this PageId
    pub fn file_id(&self) -> u8 {
        ((self.0 & Self::FILE_ID_MASK) >> Self::FILE_ID_SHIFT) as u8
    }

    /// Returns the Page Offset component of this PageId
    pub fn page_offset(&self) -> u32 {
        self.0 & Self::PAGE_OFFSET_MASK
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({}:{})", self.file_id(), self.page_offset())
    }
}

/// Frame identifier type - identifies a buffer frame in the buffer pool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FrameId(pub u32);

impl FrameId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FrameId({})", self.0)
    }
}

/// Slot identifier within a page for slotted page storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(pub u16);

impl SlotId {
    pub fn new(id: u16) -> Self {
        Self(id)
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

/// Record identifier - combination of page ID and slot ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

impl RecordId {
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }
}

/// Timestamp type for LRU-K tracking
pub type Timestamp = u64;

/// LSN (Log Sequence Number) for WAL - placeholder for future implementation
pub type Lsn = u64;

/// Invalid LSN constant
pub const INVALID_LSN: Lsn = 0;
