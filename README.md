## Crio

Crio is a relational database management system that provides a simple and efficient way to store and retrieve data. This is a brief overview of its implementation details.

### Disk Manager

The disk manager handles persistent storage using a **Multi-File Tablespace** architecture. It manages multiple database segments (e.g., `data.0`, `data.1`) and is responsible for routing I/O requests to the correct physical file. By decoupling logical pages from physical files, it enables parallelism and bypasses OS file size limits.

### Buffer Pool Manager

The Buffer Pool Manager acts as a specialized virtual memory system, creating the illusion that the entire database is resident in memory. It transparently manages a fixed set of memory frames by swapping pages in and out from disk, utilizing intelligent eviction policies like LRU-K to optimize for database access patterns.

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
- **Compaction:** Crio implements a `compact()` mechanism to handle internal fragmentation. When tuples are deleted, it leaves gaps; compaction slides active tuples together to reclaim these "holes" and maximize contiguous free space for new insertions without invalidating logical `RecordId`s.

Furthermore, Crio implements a **Free Space Map (FSM)** logic (via the `ExtentAllocator`) to track availability across pages. This allows the system to efficiently find pages with enough room for new tuples, mirroring Postgres' `pg_freespace` utility:

```sql
postgres=# SELECT *, round(100 * avail/8192 ,2) as "freespace ratio" FROM pg_freespace('student');
 blkno | avail | freespace ratio 
-------+-------+-----------------
     0 |    32 |            0.00
     5 |  4768 |           58.00
```

Each page header includes critical metadata such as the **Page ID** and **Log Sequence Number (LSN)** for self-identification and robust crash recovery via the WAL protocol.
