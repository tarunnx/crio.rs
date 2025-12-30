use std::sync::Arc;

use super::{Schema, Value};

/// Represents a single row/tuple in a table.
///
/// A tuple contains a list of values corresponding to the columns defined in its schema.
/// It provides methods for serialization to/from raw bytes for storage in pages.
///
/// ## Tuple Binary Format
///
/// The tuple is serialized in the following format:
///
/// ```text
/// +----------------+------------------+------------------+
/// | Null Bitmap    | Fixed-Size Data  | Variable-Size    |
/// | (N bytes)      | (F bytes)        | Data (V bytes)   |
/// +----------------+------------------+------------------+
/// ```
///
/// Where:
/// - **Null Bitmap**: ceil(column_count / 8) bytes, 1 bit per column (1 = NULL)
/// - **Fixed-Size Data**: All fixed-size columns serialized in order
/// - **Variable-Size Data**: All variable-size columns serialized in order
///
/// This layout ensures:
/// 1. NULL values are efficiently encoded without storing data
/// 2. Fixed-size columns can be accessed at known offsets
/// 3. Variable-size columns are stored contiguously at the end
#[derive(Debug, Clone)]
pub struct Tuple {
    /// The schema defining the structure of this tuple
    schema: Arc<Schema>,

    /// The values for each column (in schema order)
    values: Vec<Value>,
}

impl Tuple {
    /// Creates a new tuple with the given schema and values.
    ///
    /// # Panics
    /// Panics if the number of values doesn't match the schema column count.
    pub fn new(schema: Arc<Schema>, values: Vec<Value>) -> Self {
        assert_eq!(
            values.len(),
            schema.column_count(),
            "Value count must match schema column count"
        );
        Self { schema, values }
    }

    /// Creates a tuple from raw bytes using the given schema.
    pub fn from_bytes(schema: Arc<Schema>, data: &[u8]) -> Option<Self> {
        let values = Self::deserialize_values(&schema, data)?;
        Some(Self { schema, values })
    }

    /// Returns the schema of this tuple.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Returns the value at the given column index.
    pub fn value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns the value for the given column name.
    pub fn value_by_name(&self, name: &str) -> Option<&Value> {
        self.schema
            .column_index(name)
            .and_then(|i| self.values.get(i))
    }

    /// Returns all values in this tuple.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns a mutable reference to all values.
    pub fn values_mut(&mut self) -> &mut Vec<Value> {
        &mut self.values
    }

    /// Sets the value at the given column index.
    pub fn set_value(&mut self, index: usize, value: Value) -> bool {
        if index < self.values.len() {
            self.values[index] = value;
            true
        } else {
            false
        }
    }

    /// Returns the number of columns/values in this tuple.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this tuple has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Serializes the tuple to bytes for storage.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        self.serialize_values()
    }

    /// Serializes the tuple values to bytes.
    fn serialize_values(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();

        // Step 1: Write null bitmap
        let null_bitmap = self.compute_null_bitmap();
        bytes.extend_from_slice(&null_bitmap);

        // Step 2: Write fixed-size columns in order
        for (i, col) in self.schema.columns().enumerate() {
            if col.data_type().is_fixed_size() {
                let value = &self.values[i];
                if !value.is_null() {
                    let serialized = value.serialize(col.data_type())?;
                    bytes.extend(serialized);
                } else {
                    // Write zeros for NULL fixed-size columns to maintain offsets
                    let size = col.data_type().fixed_size().unwrap();
                    bytes.extend(vec![0u8; size]);
                }
            }
        }

        // Step 3: Write variable-size columns in order
        for (i, col) in self.schema.columns().enumerate() {
            if !col.data_type().is_fixed_size() {
                let value = &self.values[i];
                if !value.is_null() {
                    let serialized = value.serialize(col.data_type())?;
                    bytes.extend(serialized);
                } else {
                    // Write zero-length for NULL variable-size columns
                    bytes.extend_from_slice(&0u16.to_le_bytes());
                }
            }
        }

        Some(bytes)
    }

    /// Computes the null bitmap for the current values.
    fn compute_null_bitmap(&self) -> Vec<u8> {
        let num_bytes = self.schema.null_bitmap_size();
        let mut bitmap = vec![0u8; num_bytes];

        for (i, value) in self.values.iter().enumerate() {
            if value.is_null() {
                let byte_index = i / 8;
                let bit_index = i % 8;
                bitmap[byte_index] |= 1 << bit_index;
            }
        }

        bitmap
    }

    /// Deserializes tuple values from bytes.
    fn deserialize_values(schema: &Schema, data: &[u8]) -> Option<Vec<Value>> {
        let mut values = Vec::with_capacity(schema.column_count());
        let mut offset = 0;

        // Step 1: Read null bitmap
        let null_bitmap_size = schema.null_bitmap_size();
        if data.len() < null_bitmap_size {
            return None;
        }
        let null_bitmap = &data[..null_bitmap_size];
        offset += null_bitmap_size;

        // Helper to check if column is null
        let is_null = |col_index: usize| -> bool {
            let byte_index = col_index / 8;
            let bit_index = col_index % 8;
            (null_bitmap[byte_index] & (1 << bit_index)) != 0
        };

        // Step 2: Read fixed-size columns
        let mut fixed_values: Vec<(usize, Value)> = Vec::new();
        for (i, col) in schema.columns().enumerate() {
            if col.data_type().is_fixed_size() {
                if is_null(i) {
                    // Skip the bytes but still advance offset
                    let size = col.data_type().fixed_size().unwrap();
                    offset += size;
                    fixed_values.push((i, Value::Null));
                } else {
                    let (value, size) = Value::deserialize(&data[offset..], col.data_type())?;
                    offset += size;
                    fixed_values.push((i, value));
                }
            }
        }

        // Step 3: Read variable-size columns
        let mut variable_values: Vec<(usize, Value)> = Vec::new();
        for (i, col) in schema.columns().enumerate() {
            if !col.data_type().is_fixed_size() {
                if is_null(i) {
                    // Read zero-length marker
                    if data.len() < offset + 2 {
                        return None;
                    }
                    let len = u16::from_le_bytes([data[offset], data[offset + 1]]);
                    if len != 0 {
                        return None; // Invalid: NULL should have 0 length
                    }
                    offset += 2;
                    variable_values.push((i, Value::Null));
                } else {
                    let (value, size) = Value::deserialize(&data[offset..], col.data_type())?;
                    offset += size;
                    variable_values.push((i, value));
                }
            }
        }

        // Step 4: Merge fixed and variable values in column order
        let mut all_values: Vec<(usize, Value)> = fixed_values;
        all_values.extend(variable_values);
        all_values.sort_by_key(|(i, _)| *i);

        for (_, value) in all_values {
            values.push(value);
        }

        Some(values)
    }

    /// Creates a projection of this tuple with only the specified columns.
    pub fn project(&self, column_indices: &[usize]) -> Option<Tuple> {
        let projected_schema = self.schema.project(column_indices)?;
        let projected_values: Option<Vec<Value>> = column_indices
            .iter()
            .map(|&i| self.values.get(i).cloned())
            .collect();

        Some(Tuple::new(Arc::new(projected_schema), projected_values?))
    }

    /// Compares this tuple with another for equality on the specified columns.
    pub fn equals_on(&self, other: &Tuple, column_indices: &[usize]) -> bool {
        for &i in column_indices {
            match (self.value(i), other.value(i)) {
                (Some(a), Some(b)) if a == b => continue,
                _ => return false,
            }
        }
        true
    }

    /// Extracts a key from the tuple for the specified columns.
    /// Useful for index operations.
    pub fn key_bytes(&self, column_indices: &[usize]) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();
        for &i in column_indices {
            let col = self.schema.column(i)?;
            let value = self.value(i)?;
            let serialized = value.serialize(col.data_type())?;
            bytes.extend(serialized);
        }
        Some(bytes)
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.values == other.values
    }
}

impl Eq for Tuple {}

/// Builder for constructing tuples fluently.
pub struct TupleBuilder {
    schema: Arc<Schema>,
    values: Vec<Value>,
    current_index: usize,
}

impl TupleBuilder {
    /// Creates a new tuple builder for the given schema.
    pub fn new(schema: Arc<Schema>) -> Self {
        let count = schema.column_count();
        Self {
            schema,
            values: vec![Value::Null; count],
            current_index: 0,
        }
    }

    /// Sets the value at the current position and advances.
    pub fn value(mut self, value: impl Into<Value>) -> Self {
        if self.current_index < self.values.len() {
            self.values[self.current_index] = value.into();
            self.current_index += 1;
        }
        self
    }

    /// Sets a null value at the current position and advances.
    pub fn null(mut self) -> Self {
        if self.current_index < self.values.len() {
            self.values[self.current_index] = Value::Null;
            self.current_index += 1;
        }
        self
    }

    /// Sets the value for a specific column by name.
    pub fn set(mut self, name: &str, value: impl Into<Value>) -> Self {
        if let Some(index) = self.schema.column_index(name) {
            self.values[index] = value.into();
        }
        self
    }

    /// Sets a null value for a specific column by name.
    pub fn set_null(mut self, name: &str) -> Self {
        if let Some(index) = self.schema.column_index(name) {
            self.values[index] = Value::Null;
        }
        self
    }

    /// Builds the tuple.
    pub fn build(self) -> Tuple {
        Tuple::new(self.schema, self.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Schema::builder()
            .column("id", DataType::Integer)
            .column("name", DataType::VarChar(100))
            .nullable_column("email", DataType::VarChar(200))
            .column("age", DataType::SmallInt)
            .build_arc()
    }

    #[test]
    fn test_tuple_creation() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            schema.clone(),
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
                Value::SmallInt(30),
            ],
        );

        assert_eq!(tuple.len(), 4);
        assert_eq!(tuple.value(0), Some(&Value::Integer(1)));
        assert_eq!(
            tuple.value_by_name("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_tuple_builder() {
        let schema = create_test_schema();
        let tuple = TupleBuilder::new(schema)
            .value(42i32)
            .value("Bob")
            .null()
            .value(25i16)
            .build();

        assert_eq!(tuple.value(0), Some(&Value::Integer(42)));
        assert_eq!(tuple.value(1), Some(&Value::String("Bob".to_string())));
        assert_eq!(tuple.value(2), Some(&Value::Null));
        assert_eq!(tuple.value(3), Some(&Value::SmallInt(25)));
    }

    #[test]
    fn test_tuple_builder_by_name() {
        let schema = create_test_schema();
        let tuple = TupleBuilder::new(schema)
            .set("id", 100i32)
            .set("name", "Charlie")
            .set("age", 35i16)
            .set_null("email")
            .build();

        assert_eq!(tuple.value_by_name("id"), Some(&Value::Integer(100)));
        assert_eq!(tuple.value_by_name("email"), Some(&Value::Null));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = create_test_schema();
        let original = Tuple::new(
            schema.clone(),
            vec![
                Value::Integer(42),
                Value::String("Test User".to_string()),
                Value::String("test@example.com".to_string()),
                Value::SmallInt(25),
            ],
        );

        let bytes = original.to_bytes().unwrap();
        let recovered = Tuple::from_bytes(schema, &bytes).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_serialization_with_nulls() {
        let schema = create_test_schema();
        let original = Tuple::new(
            schema.clone(),
            vec![
                Value::Integer(1),
                Value::String("Name".to_string()),
                Value::Null, // email is nullable
                Value::SmallInt(20),
            ],
        );

        let bytes = original.to_bytes().unwrap();
        let recovered = Tuple::from_bytes(schema, &bytes).unwrap();

        assert_eq!(original, recovered);
        assert!(recovered.value(2).unwrap().is_null());
    }

    #[test]
    fn test_null_bitmap() {
        let schema = Schema::builder()
            .nullable_column("a", DataType::Integer)
            .nullable_column("b", DataType::Integer)
            .nullable_column("c", DataType::Integer)
            .nullable_column("d", DataType::Integer)
            .nullable_column("e", DataType::Integer)
            .nullable_column("f", DataType::Integer)
            .nullable_column("g", DataType::Integer)
            .nullable_column("h", DataType::Integer)
            .nullable_column("i", DataType::Integer) // 9th column, needs 2 bytes for bitmap
            .build_arc();

        assert_eq!(schema.null_bitmap_size(), 2);

        let tuple = Tuple::new(
            schema.clone(),
            vec![
                Value::Null,       // bit 0
                Value::Integer(1), // bit 1
                Value::Null,       // bit 2
                Value::Integer(3), // bit 3
                Value::Integer(4), // bit 4
                Value::Integer(5), // bit 5
                Value::Integer(6), // bit 6
                Value::Integer(7), // bit 7
                Value::Null,       // bit 8 (second byte)
            ],
        );

        let bytes = tuple.to_bytes().unwrap();
        let recovered = Tuple::from_bytes(schema, &bytes).unwrap();

        assert_eq!(tuple, recovered);
        assert!(recovered.value(0).unwrap().is_null());
        assert!(!recovered.value(1).unwrap().is_null());
        assert!(recovered.value(2).unwrap().is_null());
        assert!(recovered.value(8).unwrap().is_null());
    }

    #[test]
    fn test_projection() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            schema,
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
                Value::SmallInt(30),
            ],
        );

        let projected = tuple.project(&[0, 3]).unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.value(0), Some(&Value::Integer(1)));
        assert_eq!(projected.value(1), Some(&Value::SmallInt(30)));
    }

    #[test]
    fn test_key_bytes() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            schema,
            vec![
                Value::Integer(42),
                Value::String("Test".to_string()),
                Value::Null,
                Value::SmallInt(10),
            ],
        );

        // Extract key from id column
        let key = tuple.key_bytes(&[0]).unwrap();
        assert_eq!(key, vec![42, 0, 0, 0]); // i32 little-endian

        // Composite key
        let key = tuple.key_bytes(&[0, 3]).unwrap();
        assert_eq!(key, vec![42, 0, 0, 0, 10, 0]); // i32 + i16
    }

    #[test]
    fn test_mixed_fixed_variable_columns() {
        // Schema with interleaved fixed and variable columns
        let schema = Schema::builder()
            .column("a", DataType::Integer) // fixed
            .column("b", DataType::VarChar(50)) // variable
            .column("c", DataType::BigInt) // fixed
            .column("d", DataType::VarChar(100)) // variable
            .build_arc();

        let tuple = Tuple::new(
            schema.clone(),
            vec![
                Value::Integer(1),
                Value::String("hello".to_string()),
                Value::BigInt(1234567890),
                Value::String("world".to_string()),
            ],
        );

        let bytes = tuple.to_bytes().unwrap();
        let recovered = Tuple::from_bytes(schema, &bytes).unwrap();

        assert_eq!(tuple, recovered);
    }
}
