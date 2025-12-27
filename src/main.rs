use std::sync::Arc;

use crio::buffer::BufferPoolManager;
use crio::storage::disk::DiskManager;
use crio::storage::page::TablePage;

fn main() {
    println!("Crio - A disk-oriented RDBMS in Rust");
    println!("=====================================\n");

    // Create a temporary database file for demonstration
    let db_path = "demo.db";

    // Initialize the disk manager
    let disk_manager = Arc::new(DiskManager::new(db_path).expect("Failed to create disk manager"));
    println!("Created disk manager for: {}", db_path);

    // Create buffer pool manager with 10 frames and LRU-2 replacement
    let bpm = BufferPoolManager::new(10, 2, disk_manager);
    println!("Created buffer pool manager with 10 frames\n");

    // Allocate a new page
    let page_id = bpm.new_page().expect("Failed to allocate page");
    println!("Allocated new page: {}", page_id);

    // Write some data to the page
    {
        let mut guard = bpm
            .checked_write_page(page_id)
            .expect("Failed to get write guard")
            .expect("Page not found");

        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1); // table_id = 1

        // Insert some tuples
        let tuples = [
            b"Hello, World!".as_slice(),
            b"This is Crio DBMS",
            b"A disk-oriented database in Rust",
        ];

        for tuple in &tuples {
            let rid = page.insert_tuple(tuple).expect("Failed to insert tuple");
            println!("Inserted tuple at {:?}", rid);
        }

        println!("\nPage stats:");
        println!("  - Tuple count: {}", page.tuple_count());
        println!("  - Free space: {} bytes", page.free_space());
    }

    // Flush the page to disk
    bpm.flush_page(page_id).expect("Failed to flush page");
    println!("\nFlushed page to disk");

    // Read the data back
    {
        let guard = bpm
            .checked_read_page(page_id)
            .expect("Failed to get read guard")
            .expect("Page not found");

        let page = crio::storage::page::TablePageRef::new(guard.data());

        println!("\nReading back from page {}:", page.page_id());
        println!("  - Table ID: {}", page.table_id());
        println!("  - Tuple count: {}", page.tuple_count());

        for i in 0..page.tuple_count() {
            if let Ok(tuple) = page.get_tuple(crio::SlotId::new(i as u16)) {
                println!("  - Tuple {}: {:?}", i, String::from_utf8_lossy(tuple));
            }
        }
    }

    // Clean up
    std::fs::remove_file(db_path).ok();
    println!("\nDemo completed successfully!");
}
