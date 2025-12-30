use std::sync::Arc;

use crio::buffer::BufferPoolManager;
use crio::common::PAGE_SIZE;
use crio::storage::disk::DiskManager;
use crio::storage::page::{TablePage, TablePageRef};
use crio::tuple::{DataType, Schema, Tuple, TupleBuilder, Value};

use tempfile::NamedTempFile;

fn create_bpm(pool_size: usize) -> (Arc<BufferPoolManager>, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let disk_manager = Arc::new(DiskManager::new(temp_file.path()).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(pool_size, 2, disk_manager));
    (bpm, temp_file)
}

fn create_user_schema() -> Arc<Schema> {
    Schema::builder()
        .column("id", DataType::Integer)
        .column("name", DataType::VarChar(100))
        .nullable_column("email", DataType::VarChar(200))
        .column("age", DataType::SmallInt)
        .column("active", DataType::Boolean)
        .build_arc()
}

#[test]
fn test_tuple_to_table_page_roundtrip() {
    let (bpm, _temp) = create_bpm(10);
    let schema = create_user_schema();

    // Create a tuple
    let original = TupleBuilder::new(schema.clone())
        .value(1i32)
        .value("Alice")
        .value("alice@example.com")
        .value(30i16)
        .value(true)
        .build();

    // Serialize to bytes
    let bytes = original.to_bytes().unwrap();

    // Store in a TablePage via buffer pool
    let page_id = bpm.new_page().unwrap();
    let record_id = {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1); // table_id = 1
        page.insert_tuple(&bytes).unwrap()
    };

    // Read back from TablePage
    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        let page = TablePageRef::new(guard.data());
        let stored_bytes = page.get_tuple(record_id.slot_id).unwrap();

        // Deserialize back to tuple
        let recovered = Tuple::from_bytes(schema.clone(), stored_bytes).unwrap();

        assert_eq!(original, recovered);
        assert_eq!(
            recovered.value_by_name("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(recovered.value_by_name("age"), Some(&Value::SmallInt(30)));
    }
}

#[test]
fn test_multiple_tuples_in_page() {
    let (bpm, _temp) = create_bpm(10);
    let schema = create_user_schema();

    let page_id = bpm.new_page().unwrap();

    // Insert multiple tuples
    let tuples: Vec<Tuple> = (0..10)
        .map(|i| {
            TupleBuilder::new(schema.clone())
                .value(i as i32)
                .value(format!("User{}", i))
                .value(format!("user{}@example.com", i))
                .value((20 + i) as i16)
                .value(i % 2 == 0)
                .build()
        })
        .collect();

    let record_ids: Vec<_> = {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1);

        tuples
            .iter()
            .map(|t| {
                let bytes = t.to_bytes().unwrap();
                page.insert_tuple(&bytes).unwrap()
            })
            .collect()
    };

    // Read back and verify each tuple
    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        let page = TablePageRef::new(guard.data());

        for (i, record_id) in record_ids.iter().enumerate() {
            let stored_bytes = page.get_tuple(record_id.slot_id).unwrap();
            let recovered = Tuple::from_bytes(schema.clone(), stored_bytes).unwrap();

            assert_eq!(recovered.value(0), Some(&Value::Integer(i as i32)));
            assert_eq!(
                recovered.value(1),
                Some(&Value::String(format!("User{}", i)))
            );
        }
    }
}

#[test]
fn test_tuple_with_nulls() {
    let (bpm, _temp) = create_bpm(10);
    let schema = create_user_schema();

    let original = TupleBuilder::new(schema.clone())
        .value(42i32)
        .value("Bob")
        .null() // email is NULL
        .value(25i16)
        .value(false)
        .build();

    let bytes = original.to_bytes().unwrap();

    let page_id = bpm.new_page().unwrap();
    let record_id = {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1);
        page.insert_tuple(&bytes).unwrap()
    };

    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        let page = TablePageRef::new(guard.data());
        let stored_bytes = page.get_tuple(record_id.slot_id).unwrap();
        let recovered = Tuple::from_bytes(schema, stored_bytes).unwrap();

        assert_eq!(recovered.value_by_name("id"), Some(&Value::Integer(42)));
        assert_eq!(recovered.value_by_name("email"), Some(&Value::Null));
        assert!(recovered.value_by_name("email").unwrap().is_null());
    }
}

#[test]
fn test_tuple_projection() {
    let schema = create_user_schema();

    let tuple = TupleBuilder::new(schema)
        .value(1i32)
        .value("Alice")
        .value("alice@example.com")
        .value(30i16)
        .value(true)
        .build();

    // Project to just id and name
    let projected = tuple.project(&[0, 1]).unwrap();

    assert_eq!(projected.len(), 2);
    assert_eq!(projected.value(0), Some(&Value::Integer(1)));
    assert_eq!(
        projected.value(1),
        Some(&Value::String("Alice".to_string()))
    );
}

#[test]
fn test_tuple_key_extraction() {
    let schema = create_user_schema();

    let tuple = TupleBuilder::new(schema)
        .value(42i32)
        .value("Test")
        .null()
        .value(25i16)
        .value(true)
        .build();

    // Extract key from id column (for B+Tree indexing)
    let key = tuple.key_bytes(&[0]).unwrap();
    assert_eq!(key, vec![42, 0, 0, 0]); // i32 in little-endian
}

#[test]
fn test_schema_serialization() {
    let schema = create_user_schema();

    let bytes = schema.serialize();
    let recovered = Schema::deserialize(&bytes).unwrap();

    assert_eq!(schema.column_count(), recovered.column_count());

    for i in 0..schema.column_count() {
        let orig_col = schema.column(i).unwrap();
        let rec_col = recovered.column(i).unwrap();

        assert_eq!(orig_col.name(), rec_col.name());
        assert_eq!(orig_col.data_type(), rec_col.data_type());
        assert_eq!(orig_col.is_nullable(), rec_col.is_nullable());
    }
}

#[test]
fn test_value_comparisons() {
    use std::cmp::Ordering;

    // Integer comparisons
    assert_eq!(
        Value::Integer(10).compare(&Value::Integer(20)),
        Some(Ordering::Less)
    );
    assert_eq!(
        Value::Integer(20).compare(&Value::Integer(10)),
        Some(Ordering::Greater)
    );
    assert_eq!(
        Value::Integer(10).compare(&Value::Integer(10)),
        Some(Ordering::Equal)
    );

    // Cross-type comparisons
    assert_eq!(
        Value::TinyInt(10).compare(&Value::BigInt(10)),
        Some(Ordering::Equal)
    );

    // String comparisons
    assert_eq!(
        Value::String("abc".into()).compare(&Value::String("abd".into())),
        Some(Ordering::Less)
    );

    // Null comparisons
    assert_eq!(Value::Null.compare(&Value::Null), Some(Ordering::Equal));
    assert_eq!(Value::Integer(10).compare(&Value::Null), None);
}

#[test]
fn test_all_data_types() {
    let schema = Schema::builder()
        .column("bool_col", DataType::Boolean)
        .column("tinyint_col", DataType::TinyInt)
        .column("smallint_col", DataType::SmallInt)
        .column("int_col", DataType::Integer)
        .column("bigint_col", DataType::BigInt)
        .column("float_col", DataType::Float)
        .column("double_col", DataType::Double)
        .column("char_col", DataType::Char(10))
        .column("varchar_col", DataType::VarChar(50))
        .column("timestamp_col", DataType::Timestamp)
        .build_arc();

    let tuple = Tuple::new(
        schema.clone(),
        vec![
            Value::Boolean(true),
            Value::TinyInt(127),
            Value::SmallInt(32000),
            Value::Integer(2_000_000_000),
            Value::BigInt(9_000_000_000_000_000_000),
            Value::Float(3.14),
            Value::Double(2.718281828),
            Value::String("hello".to_string()),
            Value::String("variable length string".to_string()),
            Value::Timestamp(1703980800000000), // Some timestamp
        ],
    );

    let bytes = tuple.to_bytes().unwrap();
    let recovered = Tuple::from_bytes(schema, &bytes).unwrap();

    assert_eq!(tuple, recovered);
}

#[test]
fn test_tuple_size_calculation() {
    let schema = Schema::builder()
        .column("id", DataType::Integer) // 4 bytes fixed
        .column("name", DataType::VarChar(100)) // variable
        .column("age", DataType::SmallInt) // 2 bytes fixed
        .build_arc();

    // Null bitmap: 1 byte (3 columns)
    // Fixed: 4 + 2 = 6 bytes
    // Min size: 1 + 6 = 7 bytes
    assert_eq!(schema.null_bitmap_size(), 1);
    assert_eq!(schema.fixed_size(), 6);
    assert_eq!(schema.min_tuple_size(), 7);

    // Max size: 1 + 4 + (2 + 100) + 2 = 109 bytes
    assert_eq!(schema.max_tuple_size(), 109);
}

#[test]
fn test_large_tuple_storage() {
    let (bpm, _temp) = create_bpm(10);

    // Create a schema with a large varchar column
    let schema = Schema::builder()
        .column("id", DataType::Integer)
        .column("data", DataType::VarChar(3000))
        .build_arc();

    // Create a tuple with a large string (but not too large to fit in a page)
    let large_string = "x".repeat(2000);
    let tuple = TupleBuilder::new(schema.clone())
        .value(1i32)
        .value(large_string.clone())
        .build();

    let bytes = tuple.to_bytes().unwrap();
    assert!(bytes.len() < PAGE_SIZE - 100); // Ensure it fits

    let page_id = bpm.new_page().unwrap();
    let record_id = {
        let mut guard = bpm.checked_write_page(page_id).unwrap().unwrap();
        let mut page = TablePage::new(guard.data_mut());
        page.init(page_id, 1);
        page.insert_tuple(&bytes).unwrap()
    };

    {
        let guard = bpm.checked_read_page(page_id).unwrap().unwrap();
        let page = TablePageRef::new(guard.data());
        let stored_bytes = page.get_tuple(record_id.slot_id).unwrap();
        let recovered = Tuple::from_bytes(schema, stored_bytes).unwrap();

        assert_eq!(recovered.value(1), Some(&Value::String(large_string)));
    }
}
