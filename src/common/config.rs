/// Size of a page in bytes (4 KB)
pub const PAGE_SIZE: usize = 4096;

/// Invalid page ID constant
pub const INVALID_PAGE_ID: PageId = PageId(u32::MAX);

/// Invalid frame ID constant
pub const INVALID_FRAME_ID: FrameId = FrameId(u32::MAX);

/// Default K value for LRU-K replacement policy
pub const DEFAULT_LRUK_K: usize = 2;

/// Default buffer pool size (number of frames)
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 10;

/// Default B+ tree order (max keys per node)
pub const DEFAULT_BTREE_ORDER: usize = 128;

/// Default minimum keys per node (typically order/2)
pub const DEFAULT_BTREE_MIN_KEYS: usize = 64;

/// Key prefix size for fast comparisons
pub const KEY_PREFIX_SIZE: usize = 16;

use super::types::{FrameId, PageId};
