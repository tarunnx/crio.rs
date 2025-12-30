use std::fmt;

/// Represents the data types supported by the database.
/// Each type has a fixed or variable size and specific serialization rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Boolean type: 1 byte (0 = false, 1 = true)
    Boolean,

    /// 8-bit signed integer: 1 byte
    TinyInt,

    /// 16-bit signed integer: 2 bytes, little-endian
    SmallInt,

    /// 32-bit signed integer: 4 bytes, little-endian
    Integer,

    /// 64-bit signed integer: 8 bytes, little-endian
    BigInt,

    /// 32-bit floating point: 4 bytes, IEEE 754
    Float,

    /// 64-bit floating point: 8 bytes, IEEE 754
    Double,

    /// Fixed-length character string: exactly n bytes, space-padded
    Char(u16),

    /// Variable-length character string: up to n bytes
    /// Stored as: length (2 bytes) + data (variable)
    VarChar(u16),

    /// Timestamp: 8 bytes, microseconds since Unix epoch
    Timestamp,
}

impl DataType {
    /// Returns true if this type has a fixed size in bytes.
    pub fn is_fixed_size(&self) -> bool {
        match self {
            DataType::Boolean
            | DataType::TinyInt
            | DataType::SmallInt
            | DataType::Integer
            | DataType::BigInt
            | DataType::Float
            | DataType::Double
            | DataType::Char(_)
            | DataType::Timestamp => true,
            DataType::VarChar(_) => false,
        }
    }

    /// Returns the fixed size in bytes, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::TinyInt => Some(1),
            DataType::SmallInt => Some(2),
            DataType::Integer => Some(4),
            DataType::BigInt => Some(8),
            DataType::Float => Some(4),
            DataType::Double => Some(8),
            DataType::Char(n) => Some(*n as usize),
            DataType::Timestamp => Some(8),
            DataType::VarChar(_) => None,
        }
    }

    /// Returns the maximum size in bytes this type can occupy.
    /// For variable-length types, this includes the length prefix.
    pub fn max_size(&self) -> usize {
        match self {
            DataType::Boolean => 1,
            DataType::TinyInt => 1,
            DataType::SmallInt => 2,
            DataType::Integer => 4,
            DataType::BigInt => 8,
            DataType::Float => 4,
            DataType::Double => 8,
            DataType::Char(n) => *n as usize,
            DataType::Timestamp => 8,
            // 2 bytes for length prefix + max data length
            DataType::VarChar(n) => 2 + *n as usize,
        }
    }

    /// Returns the type ID used for serialization in the catalog.
    pub fn type_id(&self) -> u8 {
        match self {
            DataType::Boolean => 0,
            DataType::TinyInt => 1,
            DataType::SmallInt => 2,
            DataType::Integer => 3,
            DataType::BigInt => 4,
            DataType::Float => 5,
            DataType::Double => 6,
            DataType::Char(_) => 7,
            DataType::VarChar(_) => 8,
            DataType::Timestamp => 9,
        }
    }

    /// Serializes the DataType to bytes for catalog storage.
    /// Format: type_id (1 byte) + optional length (2 bytes for Char/VarChar)
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![self.type_id()];
        match self {
            DataType::Char(n) | DataType::VarChar(n) => {
                bytes.extend_from_slice(&n.to_le_bytes());
            }
            _ => {}
        }
        bytes
    }

    /// Deserializes a DataType from bytes.
    /// Returns the DataType and number of bytes consumed.
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.is_empty() {
            return None;
        }

        let type_id = data[0];
        match type_id {
            0 => Some((DataType::Boolean, 1)),
            1 => Some((DataType::TinyInt, 1)),
            2 => Some((DataType::SmallInt, 1)),
            3 => Some((DataType::Integer, 1)),
            4 => Some((DataType::BigInt, 1)),
            5 => Some((DataType::Float, 1)),
            6 => Some((DataType::Double, 1)),
            7 => {
                if data.len() < 3 {
                    return None;
                }
                let n = u16::from_le_bytes([data[1], data[2]]);
                Some((DataType::Char(n), 3))
            }
            8 => {
                if data.len() < 3 {
                    return None;
                }
                let n = u16::from_le_bytes([data[1], data[2]]);
                Some((DataType::VarChar(n), 3))
            }
            9 => Some((DataType::Timestamp, 1)),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::VarChar(n) => write!(f, "VARCHAR({})", n),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_size_types() {
        assert!(DataType::Boolean.is_fixed_size());
        assert!(DataType::Integer.is_fixed_size());
        assert!(DataType::Char(10).is_fixed_size());
        assert!(!DataType::VarChar(100).is_fixed_size());
    }

    #[test]
    fn test_size_calculations() {
        assert_eq!(DataType::Boolean.fixed_size(), Some(1));
        assert_eq!(DataType::Integer.fixed_size(), Some(4));
        assert_eq!(DataType::BigInt.fixed_size(), Some(8));
        assert_eq!(DataType::Char(20).fixed_size(), Some(20));
        assert_eq!(DataType::VarChar(100).fixed_size(), None);
        assert_eq!(DataType::VarChar(100).max_size(), 102);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let types = vec![
            DataType::Boolean,
            DataType::TinyInt,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Float,
            DataType::Double,
            DataType::Char(50),
            DataType::VarChar(255),
            DataType::Timestamp,
        ];

        for dt in types {
            let bytes = dt.serialize();
            let (recovered, _) = DataType::deserialize(&bytes).unwrap();
            assert_eq!(dt, recovered);
        }
    }

    #[test]
    fn test_display() {
        assert_eq!(DataType::Integer.to_string(), "INTEGER");
        assert_eq!(DataType::VarChar(100).to_string(), "VARCHAR(100)");
        assert_eq!(DataType::Char(10).to_string(), "CHAR(10)");
    }
}
