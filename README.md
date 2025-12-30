## Crio

Crio is a disk-oriented relational database management system implemented in Rust. Unlike in-memory databases, Crio assumes data doesn't fit entirely in RAM, so it must efficiently manage data movement between disk and memory. This is an educational implementation following database systems principles.

### Disk Manager

The disk manager handles persistent storage using a **Multi-File Tablespace** architecture. It manages multiple database segments (e.g., `data.0`, `data.1`) and is responsible for routing I/O requests to the correct physical file. By decoupling logical pages from physical files, it enables parallelism and bypasses OS file size limits.

#### Asynchronous Disk I/O

For better performance, disk I/O runs on a dedicated background worker thread via the **DiskScheduler**. Requests are queued through a bounded channel, allowing the main thread to continue processing while I/O completes. The scheduler supports both synchronous operations (with callbacks for completion notification) and fire-and-forget writes. This architecture mirrors how production databases separate I/O from query processing to maximize throughput.

### Mapping & Metadata

Crio distinguishes between two types of mapping structures:
- **Page Directory:** A persistent, on-disk structure (located at Page 0) that maps **Table IDs** to their starting **Page IDs**. It serves as the database's "Table of Contents."
- **Page Table:** A volatile, in-memory `HashMap` managed by the Buffer Pool that maps **Page IDs** to **Frame IDs** (RAM locations). It tracks which disk pages are currently cached in memory.

### Buffer Pool & LRU-K

The Buffer Pool Manager is the central component that bridges the gap between fast memory and slow disk. It maintains a fixed number of **frames** (memory slots), each capable of holding one 4KB page. The manager tracks which disk pages are currently cached using a page table, and decides which pages to evict when the pool is full.

#### Frame Management

Each frame contains metadata beyond just the page data:
- **Pin Count:** A reference counter tracking how many operations are currently using this page. Pinned pages cannot be evicted.
- **Dirty Flag:** Indicates whether the page has been modified since it was read from disk. Dirty pages must be written back before eviction.
- **Page ID:** Identifies which disk page currently occupies this frame.

#### RAII Page Guards

To prevent resource leaks, page access uses RAII guards that automatically manage pinning:
- **ReadPageGuard:** Holds a shared read lock, auto-unpins when dropped
- **WritePageGuard:** Holds an exclusive write lock, auto-marks dirty, auto-unpins when dropped

This eliminates the common bug of forgetting to unpin a page, which would eventually deadlock the buffer pool.

#### LRU-K Replacement Policy

The Buffer Pool uses **LRU-K** (specifically K=2) instead of standard LRU or CLOCK.

- **Why LRU-K?** Standard LRU and CLOCK algorithms suffer from **Sequential Flooding**. A single large query (e.g., a full table scan) can read thousands of pages once and never use them again. In standard LRU, these "one-hit wonders" would flush out all the genuinely "hot" pages (frequently accessed indices or data), destroying cache performance.
- **How it works:** LRU-K tracks the history of the last *K* accesses for each frame. Pages with fewer than K accesses are evicted first (they're likely one-off accesses). Among pages with K or more accesses, the one with the largest "backward k-distance" (longest time since the K-th previous access) is chosen. This ensures that one-off scans pass through the buffer pool without polluting the cache, preserving the data that actually matters.
- **Eviction Priority:** Frames with infinite k-distance (fewer than K accesses) are evicted before frames with finite k-distance, using earliest access timestamp as a tiebreaker.

### Sequential Prefetching

To further optimize scan performance, the Buffer Pool Manager implements **Sequential Prefetching**.

- **Access Tracking:** An internal `AccessTracker` monitors page access patterns. If it detects a contiguous sequence of accesses (defined by `SEQUENTIAL_THRESHOLD`), it triggers a prefetch operation.
- **Bulk I/O:** Instead of fetching pages one by one, the system issues a single bulk read request for multiple subsequent pages (defined by `PREFETCH_LOOKAHEAD`). This reduces the number of expensive disk seeks and leverages the operating system's ability to read larger blocks of data efficiently.
- **Eviction-Ready:** Prefetched pages are loaded into frames but left unpinned. This means they are immediately available if requested but can be easily evicted if the prediction was wrong, preventing cache pollution.

### Files and Pages

The database persists data across multiple files composed of fixed-size **4KB** pages. This size aligns with standard OS and hardware blocks, ensuring atomic I/O and efficient memory mapping.

To support multiple files, the **Page ID** is bit-packed:
- **File ID (8 bits):** Identifies the specific database segment (up to 256 files).
- **Page Offset (24 bits):** Identifies the page index within that file (up to 16 million pages per file).

This mapping mirrors how production systems like PostgreSQL handle storage. For example, `SELECT pg_relation_filepath('table')` reveals the physical location of a relation on disk, similar to how Crio resolves a `PageId` to a specific segment file:

```sql
postgres=# SELECT pg_relation_filepath('student');
 pg_relation_filepath 
----------------------
 base/5/16389
```

Each page utilizes a **slotted-page architecture** to manage variable-length tuples and metadata efficiently:
- **Slot Array:** A directory at the top of the page that grows downward, storing the physical offset and length of each tuple.
- **Tuple Data:** The actual records stored at the bottom of the page, growing upward.
- **Free Space:** The gap between the slot array and tuple data represents available space. When they meet, the page is full.
- **Compaction:** Crio implements a `compact()` mechanism to handle internal fragmentation. When tuples are deleted, it leaves gaps; compaction slides active tuples together to reclaim these "holes" and maximize contiguous free space for new insertions without invalidating logical `RecordId`s.

#### Slot ID Stability

A critical invariant of the slotted page design is that **slot IDs remain stable** even after deletions and compaction. When a tuple is deleted, its slot entry is marked as empty (length = 0) but not removed. This ensures that existing RecordIds pointing to other tuples in the same page remain valid. Empty slots are reused for future insertions before creating new slots.

#### Table Pages and Linked Lists

For table storage, pages are extended with additional metadata to form a **doubly-linked list**. Each TablePage contains pointers to the next and previous pages, the table ID it belongs to, and an LSN for recovery. This allows efficient sequential scans and simplifies table management when pages are added or removed.

Furthermore, Crio implements a **Free Space Map (FSM)** logic (via the `ExtentAllocator`) to track availability across pages. This allows the system to efficiently find pages with enough room for new tuples, mirroring Postgres' `pg_freespace` utility:

```sql
postgres=# SELECT *, round(100 * avail/8192 ,2) as "freespace ratio" FROM pg_freespace('student');
 blkno | avail | freespace ratio 
-------+-------+-----------------
     0 |    32 |            0.00
     5 |  4768 |           58.00
```

Each page header includes critical metadata such as the **Page ID** and **Log Sequence Number (LSN)** for self-identification and robust crash recovery via the WAL protocol.

### Record Identification

Every tuple in the database is uniquely identified by a **RecordId**, which combines:
- **Page ID:** Which page contains the tuple (including file ID and page offset)
- **Slot ID:** Which slot within that page points to the tuple

This two-level addressing scheme is what B+ tree indexes store as values. When an index lookup returns a RecordId, the system can directly fetch the tuple by reading the specified page and extracting the tuple at the given slot offset. The RecordId remains valid across compaction operations because slot IDs are stable.

### Access Methods & Database Indexes

**Access methods** are data structures that organize how data is stored and retrieved. They operate as a layer on top of the Buffer Pool Manager, using it to read and write pages without knowing about disk I/O details.

#### The Layer Stack

```
┌─────────────────────────────────┐
│   Access Methods (B+ Tree)      │  ← Search, Insert, Range Scans
├─────────────────────────────────┤
│   Buffer Pool Manager           │  ← checked_read_page(), LRU-K, Prefetch
├─────────────────────────────────┤
│   Disk Manager                  │  ← Multi-file I/O
└─────────────────────────────────┘
```

**How Access Methods Use Buffer Pool:**
- Call `bpm.checked_read_page(page_id)` to get a page (auto-pins, RwLock read lock)
- Call `bpm.checked_write_page(page_id)` to modify (auto-pins, exclusive write lock, marks dirty)
- PageGuards auto-unpin via RAII when dropped
- No direct disk I/O - buffer pool handles all caching and persistence

The access method layer doesn't care whether a page is in cache or on disk, whether it's in file `data.0` or `data.1`, or how the LRU-K replacement policy works. It simply says "give me page 100" and the buffer pool handles everything.

#### Why B+ Trees?

B+ trees are the dominant index structure in databases for several reasons:

1. **Balanced Search**: O(log N) guaranteed - all leaves at same depth
   - With 4KB pages and 128-key order: 3-4 levels for millions of records
   - Root and upper internal nodes stay cached in buffer pool

2. **Range Query Efficiency**: Leaf nodes form a doubly-linked list
   - Find start key: O(log N) tree traversal
   - Scan range: follow `next_page_id` pointers through leaves
   - Combined with Sequential Prefetching: near-sequential I/O throughput

3. **High Fan-out**: Large pages store many keys per node
   - Fewer levels = fewer disk reads to reach leaves
   - Internal nodes highly cacheable (frequently accessed)

4. **Write Efficiency**: Splits are rare in practice
   - Amortized O(1) splits per insert
   - Most inserts just modify one leaf page
   - Sequential inserts benefit from rightmost path optimization

**Example:**
```sql
SELECT * FROM students WHERE age BETWEEN 20 AND 25;
```

With B+ tree index on `age`:
- Navigate tree to age=20 (3-4 page reads via buffer pool)
- Follow leaf links until age>25 (sequential page access)
- Prefetching loads next pages during scan
- Total: ~10-20 page reads

Without index (heap file scan):
- Must read ALL table pages (could be thousands!)
- No way to skip irrelevant data

#### B+ Tree in Crio

The B+ tree implementation uses the same patterns as TablePage:

```rust
// Search for a key
let index = BTreeIndex::open(index_id, root_page_id, bpm)?;
let record_id = index.search(key)?;

// Fetch actual tuple from heap file
let guard = bpm.checked_read_page(record_id.page_id)?;
let page = TablePageRef::new(guard.data());
let tuple = page.get_tuple(record_id.slot_id)?;
```

Internal nodes and leaf nodes are stored as `BTreeNode` pages, managed by the buffer pool just like `TablePage`. The index layer coordinates tree navigation and node splits, but relies entirely on the buffer pool for I/O, caching, concurrency control (via RwLocks), and persistence.

#### B+ Tree Node Layout

Each B+ tree node is stored in a single page with the following structure:
- **Header (20 bytes):** Contains page ID, is_leaf flag, key count, next/prev leaf pointers (for range scans), and parent pointer (for splits)
- **Keys:** Stored contiguously after the header, 4 bytes each for integer keys
- **Values/Children:** For leaf nodes, RecordIds (6 bytes each: PageId + SlotId). For internal nodes, child PageIds (4 bytes each)

The dynamic layout places values/children immediately after the keys section. This means when keys are inserted or removed, the values/children region must shift accordingly. Internal nodes have N+1 children for N keys, with child[i] pointing to keys less than key[i].

#### Node Splitting

When a node exceeds the maximum key count (order = 128 by default), it splits:
1. The node is divided at the midpoint
2. The left half stays in the original page
3. The right half moves to a newly allocated page
4. A separator key is promoted to the parent
5. If the parent overflows, it recursively splits
6. If the root splits, a new root is created, increasing tree height by one

Leaf splits also update the doubly-linked list pointers to maintain range scan capability.

### Concurrency Model

Crio uses a layered approach to thread safety:

- **Atomic Operations:** Pin counts and dirty flags use lock-free atomics for high-frequency operations
- **RwLocks:** Page data uses reader-writer locks allowing multiple concurrent readers or one exclusive writer
- **Mutexes:** Shared state like the page table and free list use mutexes for exclusive access
- **Channels:** The disk scheduler uses bounded crossbeam channels for work queue management
- **Arc:** Shared ownership of frames and managers across threads

The RAII guard pattern ensures locks are held only as long as needed and automatically released, preventing common concurrency bugs like forgotten unlocks or deadlocks from lock ordering violations.

### Query Execution Example

To illustrate how all layers work together, consider executing `SELECT * FROM users WHERE id = 42`:

1. **Index Lookup:** The B+ tree index on `id` is traversed. Starting from the root (likely cached in buffer pool), internal nodes are read to find the leaf containing key 42.

2. **Buffer Pool Interaction:** Each node access calls `checked_read_page()`. If the page is cached, it returns immediately. If not, a free frame is found (possibly evicting a victim via LRU-K), and the disk scheduler reads the page.

3. **Leaf Search:** Binary search within the leaf node finds key 42 and returns its RecordId (e.g., PageId(10), SlotId(5)).

4. **Tuple Fetch:** The data page (PageId 10) is fetched through the buffer pool. The slotted page structure is parsed to find slot 5's offset and length, and the tuple bytes are extracted.

5. **Guard Cleanup:** As each PageGuard goes out of scope, frames are unpinned and marked evictable in the LRU-K replacer.

The entire operation might require 4-5 page reads (3-4 index levels + 1 data page), but with a warm cache, only the final data page read might hit disk.
