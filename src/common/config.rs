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

use super::types::{FrameId, PageId};
