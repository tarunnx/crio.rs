## Crio

Crio is a relational database management system that provides a simple and efficient way to store and retrieve data. This is a brief overview of its implementation details.

### Disk Manager

The disk manager is responsible for managing the storage of data on disk. It provides an interface for reading and writing pages to disk.

### Buffer Pool Manager

The Buffer Pool Manager acts as a specialized virtual memory system, creating the illusion that the entire database is resident in memory. It transparently manages a fixed set of memory frames by swapping pages in and out from disk, utilizing intelligent eviction policies like LRU-K to optimize for database access patterns.

### Files and Pages

The database persists data in a single file composed of fixed-size pages, currently set to **4KB**. This size corresponds to the standard page size of most operating systems and hardware (SSDs/HDDs), ensuring:
- **Atomic I/O:** Reading or writing a single database page maps directly to a single OS memory page and typically a single hardware block, minimizing write amplification and "torn page" risks.
- **Efficient Memory Management:** The buffer pool frames align perfectly with the OS virtual memory system.

Each page utilizes a slotted-page architecture to manage variable-length tuples and metadata efficiently. Each page header includes critical metadata such as the **Page ID** and **Log Sequence Number (LSN)**. This enables self-identification to detect file system corruption and supports robust crash recovery via the Write-Ahead Logging (WAL) protocol, ensuring data consistency by tracking which modifications have been persisted to disk.
