## Database internals

The implementation details of a relational database management system.

### Disk Manager

The disk manager is responsible for managing the storage of data on disk. It provides an interface for reading and writing pages to disk.

### Buffer Pool Manager

The Buffer Pool Manager acts as a specialized virtual memory system, creating the illusion that the entire database is resident in memory. It transparently manages a fixed set of memory frames by swapping pages in and out from disk, utilizing intelligent eviction policies like LRU-K to optimize for database access patterns.

### Files and Pages

The database persists data in a single file composed of fixed-size pages. Each page serves as the fundamental unit of I/O and storage, utilizing a slotted-page architecture to manage variable-length tuples and metadata efficiently. This structure ensures a mapping between logical record identifiers and physical disk offsets.

Each page header includes critical metadata such as the **Page ID** and **Log Sequence Number (LSN)**. This enables self-identification to detect file system corruption and supports robust crash recovery via the Write-Ahead Logging (WAL) protocol, ensuring data consistency by tracking which modifications have been persisted to disk.
