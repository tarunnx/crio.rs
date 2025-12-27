//! Crio - A disk-oriented RDBMS implementation in Rust
//!
//! This crate provides the core components for a relational database management system
//! with a disk-oriented architecture. The DBMS stores data on persistent storage and
//! uses a buffer pool to cache frequently accessed pages in memory.
//!
//! # Architecture
//!
//! The system is organized into several layers:
//!
//! - **Storage Layer** (`storage`): Handles disk I/O and page organization
//!   - `DiskManager`: Reads and writes pages to/from disk
//!   - `DiskScheduler`: Asynchronous disk I/O scheduling
//!   - `SlottedPage`: Variable-length tuple storage within pages
//!   - `TablePage`: Table-specific page format with linked list structure
//!
//! - **Buffer Pool** (`buffer`): Memory management for database pages
//!   - `BufferPoolManager`: Fetches pages from disk and caches them in memory
//!   - `LruKReplacer`: LRU-K page replacement policy
//!   - `FrameHeader`: Per-frame metadata and data storage
//!   - `ReadPageGuard`/`WritePageGuard`: RAII guards for thread-safe page access
//!
//! - **Catalog** (`catalog`): System catalog and metadata management (TODO)
//!
//! - **Execution** (`execution`): Query execution engine (TODO)
//!
//! - **Index** (`index`): Index structures like B+Tree (TODO)
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use crio::buffer::BufferPoolManager;
//! use crio::storage::disk::DiskManager;
//! use crio::storage::page::TablePage;
//! use crio::common::PageId;
//!
//! // Create a disk manager for a database file
//! let disk_manager = Arc::new(DiskManager::new("test.db").unwrap());
//!
//! // Create a buffer pool with 100 frames and LRU-2 replacement
//! let bpm = BufferPoolManager::new(100, 2, disk_manager);
//!
//! // Allocate a new page
//! let page_id = bpm.new_page().unwrap();
//!
//! // Write data to the page
//! {
//!     let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
//!     let mut page = TablePage::new(guard.data_mut());
//!     page.init(page_id, 1); // table_id = 1
//!     page.insert_tuple(b"Hello, World!").unwrap();
//! }
//!
//! // Flush changes to disk
//! bpm.flush_page(page_id).unwrap();
//! ```

pub mod buffer;
pub mod catalog;
pub mod common;
pub mod execution;
pub mod index;
pub mod storage;

// Re-export commonly used types at the crate root
pub use common::{CrioError, PageId, RecordId, Result, SlotId};
