## Database internals

The implementation details of a relational database management system.

### Disk Manager

The disk manager is responsible for managing the storage of data on disk. It provides an interface for reading and writing pages to disk.

### Buffer Pool Manager

The Buffer Pool Manager acts as a specialized virtual memory system, creating the illusion that the entire database is resident in memory. It transparently manages a fixed set of memory frames by swapping pages in and out from disk, utilizing intelligent eviction policies like LRU-K to optimize for database access patterns.
