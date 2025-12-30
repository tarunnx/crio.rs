use thiserror::Error;

use super::types::{FrameId, PageId};

/// Database error types
#[derive(Error, Debug)]
pub enum CrioError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page {0} not found")]
    PageNotFound(PageId),

    #[error("Frame {0} not found")]
    FrameNotFound(FrameId),

    #[error("Buffer pool is full, no evictable frames available")]
    BufferPoolFull,

    #[error("Invalid page ID: {0}")]
    InvalidPageId(PageId),

    #[error("Invalid frame ID: {0}")]
    InvalidFrameId(FrameId),

    #[error("Page {0} is still pinned")]
    PageStillPinned(PageId),

    #[error("Failed to evict page")]
    EvictionFailed,

    #[error("Disk scheduler error: {0}")]
    DiskScheduler(String),

    #[error("Page overflow: tuple size {tuple_size} exceeds available space {available}")]
    PageOverflow { tuple_size: usize, available: usize },

    #[error("Invalid slot ID: {0}")]
    InvalidSlotId(u16),

    #[error("Slot {0} is empty")]
    EmptySlot(u16),

    #[error("Page is full")]
    PageFull,

    #[error("Lock poisoned")]
    LockPoisoned,

    #[error("Channel error: {0}")]
    Channel(String),

    #[error("Table {0} already exists")]
    TableAlreadyExists(u32),

    #[error("Table {0} not found")]
    TableNotFound(u32),

    #[error("Directory page is full")]
    DirectoryFull,

    #[error("Invalid database file")]
    InvalidDatabaseFile,

    #[error("Duplicate key: {0}")]
    DuplicateKey(u32),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Index {0} not found")]
    IndexNotFound(u32),

    #[error("Index corrupted: {0}")]
    IndexCorrupted(String),
}

pub type Result<T> = std::result::Result<T, CrioError>;
