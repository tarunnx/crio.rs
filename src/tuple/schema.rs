use std::collections::HashMap;
use std::sync::Arc;

use super::DataType;

/// Represents a single column in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Column name
    name: String,

    /// Column data type
    data_type: DataType,

    /// Whether the column allows NULL values
    nullable: bool,

    /// Column position in the schema (0-indexed)
    ordinal: usize,
}

impl Column {
    /// Creates a new column definition.
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            ordinal: 0, // Will be set by Schema
        }
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column data type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns whether the column allows NULL values.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the column's ordinal position in the schema.
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    /// Returns the fixed size of this column, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        self.data_type.fixed_size()
    }

    /// Returns the maximum size this column can occupy in bytes.
    pub fn max_size(&self) -> usize {
        self.data_type.max_size()
    }

    /// Serializes the column definition to bytes.
    /// Format: name_len (2 bytes) + name + data_type + nullable (1 byte)
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Name length and name
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(name_bytes);

        // Data type
        bytes.extend(self.data_type.serialize());

        // Nullable flag
        bytes.push(if self.nullable { 1 } else { 0 });

        bytes
    }

    /// Deserializes a column definition from bytes.
    /// Returns the column and number of bytes consumed.
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 2 {
            return None;
        }

        let mut offset = 0;

        // Name length and name
        let name_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        offset += 2;

        if data.len() < offset + name_len {
            return None;
        }
        let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
        offset += name_len;

        // Data type
        let (data_type, dt_size) = DataType::deserialize(&data[offset..])?;
        offset += dt_size;

        // Nullable flag
        if data.len() < offset + 1 {
            return None;
        }
        let nullable = data[offset] != 0;
        offset += 1;

        Some((
            Column {
                name,
                data_type,
                nullable,
                ordinal: 0, // Will be set by Schema
            },
            offset,
        ))
    }
}

/// Represents the schema of a table, defining its columns and structure.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Ordered list of columns
    columns: Vec<Column>,

    /// Map from column name to column index for fast lookup
    name_to_index: HashMap<String, usize>,

    /// Total size of fixed-length columns
    fixed_size: usize,

    /// Number of variable-length columns
    variable_count: usize,

    /// Size of the null bitmap in bytes (ceiling of column_count / 8)
    null_bitmap_size: usize,
}

impl Schema {
    /// Creates a new schema from a list of columns.
    pub fn new(columns: Vec<Column>) -> Self {
        let mut columns = columns;
        let mut name_to_index = HashMap::new();
        let mut fixed_size = 0;
        let mut variable_count = 0;

        // Assign ordinals and build index
        for (i, col) in columns.iter_mut().enumerate() {
            col.ordinal = i;
            name_to_index.insert(col.name.clone(), i);

            if let Some(size) = col.fixed_size() {
                fixed_size += size;
            } else {
                variable_count += 1;
            }
        }

        // Null bitmap: 1 bit per column, rounded up to bytes
        let null_bitmap_size = (columns.len() + 7) / 8;

        Self {
            columns,
            name_to_index,
            fixed_size,
            variable_count,
            null_bitmap_size,
        }
    }

    /// Creates a schema builder for fluent construction.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Returns the number of columns in the schema.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the column at the given index.
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Returns the column with the given name.
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.name_to_index
            .get(name)
            .and_then(|&i| self.columns.get(i))
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Returns an iterator over all columns.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    /// Returns the total size of all fixed-length columns.
    pub fn fixed_size(&self) -> usize {
        self.fixed_size
    }

    /// Returns the number of variable-length columns.
    pub fn variable_count(&self) -> usize {
        self.variable_count
    }

    /// Returns the size of the null bitmap in bytes.
    pub fn null_bitmap_size(&self) -> usize {
        self.null_bitmap_size
    }

    /// Returns the minimum tuple size (null bitmap + fixed columns).
    pub fn min_tuple_size(&self) -> usize {
        self.null_bitmap_size + self.fixed_size
    }

    /// Returns the maximum tuple size including all variable-length columns at max capacity.
    pub fn max_tuple_size(&self) -> usize {
        self.null_bitmap_size + self.columns.iter().map(|c| c.max_size()).sum::<usize>()
    }

    /// Serializes the schema to bytes for catalog storage.
    /// Format: column_count (2 bytes) + [column_data...]
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Column count
        bytes.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        // Each column
        for col in &self.columns {
            bytes.extend(col.serialize());
        }

        bytes
    }

    /// Deserializes a schema from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }

        let column_count = u16::from_le_bytes([data[0], data[1]]) as usize;
        let mut offset = 2;
        let mut columns = Vec::with_capacity(column_count);

        for _ in 0..column_count {
            let (col, col_size) = Column::deserialize(&data[offset..])?;
            columns.push(col);
            offset += col_size;
        }

        Some(Schema::new(columns))
    }

    /// Creates a projection of this schema with only the specified columns.
    pub fn project(&self, column_indices: &[usize]) -> Option<Schema> {
        let columns: Option<Vec<Column>> = column_indices
            .iter()
            .map(|&i| self.columns.get(i).cloned())
            .collect();

        columns.map(Schema::new)
    }

    /// Creates a projection of this schema with only the named columns.
    pub fn project_by_name(&self, column_names: &[&str]) -> Option<Schema> {
        let indices: Option<Vec<usize>> = column_names
            .iter()
            .map(|name| self.column_index(name))
            .collect();

        indices.and_then(|i| self.project(&i))
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

impl Eq for Schema {}

/// Builder for constructing schemas fluently.
pub struct SchemaBuilder {
    columns: Vec<Column>,
}

impl SchemaBuilder {
    /// Creates a new schema builder.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Adds a non-nullable column.
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type, false));
        self
    }

    /// Adds a nullable column.
    pub fn nullable_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type, true));
        self
    }

    /// Adds a column with explicit nullability.
    pub fn add_column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        self.columns.push(Column::new(name, data_type, nullable));
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Schema {
        Schema::new(self.columns)
    }

    /// Builds the schema wrapped in an Arc for shared ownership.
    pub fn build_arc(self) -> Arc<Schema> {
        Arc::new(self.build())
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        Schema::builder()
            .column("id", DataType::Integer)
            .column("name", DataType::VarChar(100))
            .nullable_column("email", DataType::VarChar(200))
            .column("age", DataType::SmallInt)
            .build()
    }

    #[test]
    fn test_schema_creation() {
        let schema = create_test_schema();

        assert_eq!(schema.column_count(), 4);
        assert_eq!(schema.column(0).unwrap().name(), "id");
        assert_eq!(schema.column(1).unwrap().name(), "name");
        assert_eq!(schema.column(2).unwrap().name(), "email");
        assert_eq!(schema.column(3).unwrap().name(), "age");
    }

    #[test]
    fn test_column_lookup() {
        let schema = create_test_schema();

        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("nonexistent"), None);

        let col = schema.column_by_name("email").unwrap();
        assert!(col.is_nullable());
        assert_eq!(*col.data_type(), DataType::VarChar(200));
    }

    #[test]
    fn test_ordinals() {
        let schema = create_test_schema();

        for (i, col) in schema.columns().enumerate() {
            assert_eq!(col.ordinal(), i);
        }
    }

    #[test]
    fn test_size_calculations() {
        let schema = create_test_schema();

        // Fixed size: id (4) + age (2) = 6
        assert_eq!(schema.fixed_size(), 6);

        // Variable count: name + email = 2
        assert_eq!(schema.variable_count(), 2);

        // Null bitmap: 4 columns = 1 byte
        assert_eq!(schema.null_bitmap_size(), 1);

        // Min tuple size: null bitmap (1) + fixed (6) = 7
        assert_eq!(schema.min_tuple_size(), 7);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let schema = create_test_schema();
        let bytes = schema.serialize();
        let recovered = Schema::deserialize(&bytes).unwrap();

        assert_eq!(schema, recovered);
    }

    #[test]
    fn test_projection() {
        let schema = create_test_schema();

        let projected = schema.project(&[0, 2]).unwrap();
        assert_eq!(projected.column_count(), 2);
        assert_eq!(projected.column(0).unwrap().name(), "id");
        assert_eq!(projected.column(1).unwrap().name(), "email");

        let projected_by_name = schema.project_by_name(&["name", "age"]).unwrap();
        assert_eq!(projected_by_name.column_count(), 2);
        assert_eq!(projected_by_name.column(0).unwrap().name(), "name");
        assert_eq!(projected_by_name.column(1).unwrap().name(), "age");
    }

    #[test]
    fn test_column_serialization() {
        let col = Column::new("test_col", DataType::VarChar(50), true);
        let bytes = col.serialize();
        let (recovered, _) = Column::deserialize(&bytes).unwrap();

        assert_eq!(col.name(), recovered.name());
        assert_eq!(col.data_type(), recovered.data_type());
        assert_eq!(col.is_nullable(), recovered.is_nullable());
    }
}
