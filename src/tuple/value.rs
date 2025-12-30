use std::cmp::Ordering;
use std::fmt;

use super::DataType;

/// Represents a typed value that can be stored in a tuple.
/// Each variant corresponds to a DataType and holds the actual data.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value - can be any type
    Null,

    /// Boolean value
    Boolean(bool),

    /// 8-bit signed integer
    TinyInt(i8),

    /// 16-bit signed integer
    SmallInt(i16),

    /// 32-bit signed integer
    Integer(i32),

    /// 64-bit signed integer
    BigInt(i64),

    /// 32-bit floating point
    Float(f32),

    /// 64-bit floating point
    Double(f64),

    /// String value (used for both Char and VarChar)
    String(String),

    /// Timestamp value (microseconds since Unix epoch)
    Timestamp(i64),
}

impl Value {
    /// Returns the DataType that best matches this value.
    /// Note: String returns VarChar with the string's length.
    pub fn infer_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::TinyInt(_) => Some(DataType::TinyInt),
            Value::SmallInt(_) => Some(DataType::SmallInt),
            Value::Integer(_) => Some(DataType::Integer),
            Value::BigInt(_) => Some(DataType::BigInt),
            Value::Float(_) => Some(DataType::Float),
            Value::Double(_) => Some(DataType::Double),
            Value::String(s) => Some(DataType::VarChar(s.len() as u16)),
            Value::Timestamp(_) => Some(DataType::Timestamp),
        }
    }

    /// Returns true if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Serializes a value to bytes according to the given DataType.
    /// Returns None if the value is incompatible with the type.
    pub fn serialize(&self, data_type: &DataType) -> Option<Vec<u8>> {
        match (self, data_type) {
            (Value::Null, _) => {
                // Null values are handled by the null bitmap, not serialized here
                // Return empty bytes; the tuple layer handles null encoding
                Some(Vec::new())
            }

            (Value::Boolean(b), DataType::Boolean) => Some(vec![if *b { 1 } else { 0 }]),

            (Value::TinyInt(v), DataType::TinyInt) => Some(v.to_le_bytes().to_vec()),

            (Value::SmallInt(v), DataType::SmallInt) => Some(v.to_le_bytes().to_vec()),

            (Value::Integer(v), DataType::Integer) => Some(v.to_le_bytes().to_vec()),

            (Value::BigInt(v), DataType::BigInt) => Some(v.to_le_bytes().to_vec()),

            (Value::Float(v), DataType::Float) => Some(v.to_le_bytes().to_vec()),

            (Value::Double(v), DataType::Double) => Some(v.to_le_bytes().to_vec()),

            (Value::String(s), DataType::Char(n)) => {
                let n = *n as usize;
                let bytes = s.as_bytes();
                if bytes.len() > n {
                    return None; // String too long
                }
                // Pad with spaces to fixed length
                let mut result = bytes.to_vec();
                result.resize(n, b' ');
                Some(result)
            }

            (Value::String(s), DataType::VarChar(max_len)) => {
                let bytes = s.as_bytes();
                if bytes.len() > *max_len as usize {
                    return None; // String too long
                }
                // Format: length (2 bytes) + data
                let len = bytes.len() as u16;
                let mut result = len.to_le_bytes().to_vec();
                result.extend_from_slice(bytes);
                Some(result)
            }

            (Value::Timestamp(v), DataType::Timestamp) => Some(v.to_le_bytes().to_vec()),

            // Type coercions
            (Value::TinyInt(v), DataType::SmallInt) => Some((*v as i16).to_le_bytes().to_vec()),
            (Value::TinyInt(v), DataType::Integer) => Some((*v as i32).to_le_bytes().to_vec()),
            (Value::TinyInt(v), DataType::BigInt) => Some((*v as i64).to_le_bytes().to_vec()),
            (Value::SmallInt(v), DataType::Integer) => Some((*v as i32).to_le_bytes().to_vec()),
            (Value::SmallInt(v), DataType::BigInt) => Some((*v as i64).to_le_bytes().to_vec()),
            (Value::Integer(v), DataType::BigInt) => Some((*v as i64).to_le_bytes().to_vec()),
            (Value::Float(v), DataType::Double) => Some((*v as f64).to_le_bytes().to_vec()),

            _ => None, // Incompatible types
        }
    }

    /// Deserializes a value from bytes according to the given DataType.
    /// Returns the value and number of bytes consumed.
    pub fn deserialize(data: &[u8], data_type: &DataType) -> Option<(Self, usize)> {
        match data_type {
            DataType::Boolean => {
                if data.is_empty() {
                    return None;
                }
                Some((Value::Boolean(data[0] != 0), 1))
            }

            DataType::TinyInt => {
                if data.is_empty() {
                    return None;
                }
                Some((Value::TinyInt(data[0] as i8), 1))
            }

            DataType::SmallInt => {
                if data.len() < 2 {
                    return None;
                }
                let v = i16::from_le_bytes([data[0], data[1]]);
                Some((Value::SmallInt(v), 2))
            }

            DataType::Integer => {
                if data.len() < 4 {
                    return None;
                }
                let v = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Some((Value::Integer(v), 4))
            }

            DataType::BigInt => {
                if data.len() < 8 {
                    return None;
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Some((Value::BigInt(v), 8))
            }

            DataType::Float => {
                if data.len() < 4 {
                    return None;
                }
                let v = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Some((Value::Float(v), 4))
            }

            DataType::Double => {
                if data.len() < 8 {
                    return None;
                }
                let v = f64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Some((Value::Double(v), 8))
            }

            DataType::Char(n) => {
                let n = *n as usize;
                if data.len() < n {
                    return None;
                }
                // Trim trailing spaces
                let s = String::from_utf8_lossy(&data[..n]).trim_end().to_string();
                Some((Value::String(s), n))
            }

            DataType::VarChar(_) => {
                if data.len() < 2 {
                    return None;
                }
                let len = u16::from_le_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + len {
                    return None;
                }
                let s = String::from_utf8_lossy(&data[2..2 + len]).to_string();
                Some((Value::String(s), 2 + len))
            }

            DataType::Timestamp => {
                if data.len() < 8 {
                    return None;
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Some((Value::Timestamp(v), 8))
            }
        }
    }

    /// Compares two values for ordering.
    /// Returns None if the values are not comparable (different types or null).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) | (_, Value::Null) => None,

            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            (Value::TinyInt(a), Value::TinyInt(b)) => Some(a.cmp(b)),
            (Value::SmallInt(a), Value::SmallInt(b)) => Some(a.cmp(b)),
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),

            // Cross-type numeric comparisons (promote to larger type)
            (Value::TinyInt(a), Value::SmallInt(b)) => Some((*a as i16).cmp(b)),
            (Value::SmallInt(a), Value::TinyInt(b)) => Some(a.cmp(&(*b as i16))),
            (Value::TinyInt(a), Value::Integer(b)) => Some((*a as i32).cmp(b)),
            (Value::Integer(a), Value::TinyInt(b)) => Some(a.cmp(&(*b as i32))),
            (Value::TinyInt(a), Value::BigInt(b)) => Some((*a as i64).cmp(b)),
            (Value::BigInt(a), Value::TinyInt(b)) => Some(a.cmp(&(*b as i64))),
            (Value::SmallInt(a), Value::Integer(b)) => Some((*a as i32).cmp(b)),
            (Value::Integer(a), Value::SmallInt(b)) => Some(a.cmp(&(*b as i32))),
            (Value::SmallInt(a), Value::BigInt(b)) => Some((*a as i64).cmp(b)),
            (Value::BigInt(a), Value::SmallInt(b)) => Some(a.cmp(&(*b as i64))),
            (Value::Integer(a), Value::BigInt(b)) => Some((*a as i64).cmp(b)),
            (Value::BigInt(a), Value::Integer(b)) => Some(a.cmp(&(*b as i64))),

            (Value::Float(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::Float(b)) => a.partial_cmp(&(*b as f64)),

            _ => None, // Incompatible types
        }
    }

    /// Attempts to cast this value to the target type.
    /// Returns None if the cast is not possible.
    pub fn cast(&self, target: &DataType) -> Option<Value> {
        match (self, target) {
            (Value::Null, _) => Some(Value::Null),

            // Integer promotions
            (Value::TinyInt(v), DataType::SmallInt) => Some(Value::SmallInt(*v as i16)),
            (Value::TinyInt(v), DataType::Integer) => Some(Value::Integer(*v as i32)),
            (Value::TinyInt(v), DataType::BigInt) => Some(Value::BigInt(*v as i64)),
            (Value::SmallInt(v), DataType::Integer) => Some(Value::Integer(*v as i32)),
            (Value::SmallInt(v), DataType::BigInt) => Some(Value::BigInt(*v as i64)),
            (Value::Integer(v), DataType::BigInt) => Some(Value::BigInt(*v as i64)),

            // Float promotions
            (Value::Float(v), DataType::Double) => Some(Value::Double(*v as f64)),

            // Integer to float
            (Value::TinyInt(v), DataType::Float) => Some(Value::Float(*v as f32)),
            (Value::TinyInt(v), DataType::Double) => Some(Value::Double(*v as f64)),
            (Value::SmallInt(v), DataType::Float) => Some(Value::Float(*v as f32)),
            (Value::SmallInt(v), DataType::Double) => Some(Value::Double(*v as f64)),
            (Value::Integer(v), DataType::Float) => Some(Value::Float(*v as f32)),
            (Value::Integer(v), DataType::Double) => Some(Value::Double(*v as f64)),
            (Value::BigInt(v), DataType::Float) => Some(Value::Float(*v as f32)),
            (Value::BigInt(v), DataType::Double) => Some(Value::Double(*v as f64)),

            // String conversions
            (Value::String(s), DataType::Char(n)) => {
                if s.len() <= *n as usize {
                    Some(Value::String(s.clone()))
                } else {
                    None
                }
            }
            (Value::String(s), DataType::VarChar(n)) => {
                if s.len() <= *n as usize {
                    Some(Value::String(s.clone()))
                } else {
                    None
                }
            }

            // Same type - no conversion needed
            (v, dt) if v.infer_type().as_ref() == Some(dt) => Some(v.clone()),

            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::TinyInt(v) => write!(f, "{}", v),
            Value::SmallInt(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::BigInt(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "'{}'", s),
            Value::Timestamp(v) => write!(f, "TIMESTAMP({})", v),
        }
    }
}

// Convenience conversions
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::TinyInt(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::SmallInt(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_serialization() {
        let val = Value::Integer(42);
        let bytes = val.serialize(&DataType::Integer).unwrap();
        assert_eq!(bytes, vec![42, 0, 0, 0]);

        let (recovered, size) = Value::deserialize(&bytes, &DataType::Integer).unwrap();
        assert_eq!(recovered, val);
        assert_eq!(size, 4);
    }

    #[test]
    fn test_varchar_serialization() {
        let val = Value::String("hello".to_string());
        let bytes = val.serialize(&DataType::VarChar(100)).unwrap();
        assert_eq!(bytes, vec![5, 0, b'h', b'e', b'l', b'l', b'o']);

        let (recovered, size) = Value::deserialize(&bytes, &DataType::VarChar(100)).unwrap();
        assert_eq!(recovered, val);
        assert_eq!(size, 7);
    }

    #[test]
    fn test_char_serialization() {
        let val = Value::String("hi".to_string());
        let bytes = val.serialize(&DataType::Char(5)).unwrap();
        assert_eq!(bytes, vec![b'h', b'i', b' ', b' ', b' ']);

        let (recovered, size) = Value::deserialize(&bytes, &DataType::Char(5)).unwrap();
        assert_eq!(recovered, Value::String("hi".to_string()));
        assert_eq!(size, 5);
    }

    #[test]
    fn test_comparison() {
        assert_eq!(
            Value::Integer(10).compare(&Value::Integer(20)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Integer(10).compare(&Value::BigInt(5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::String("abc".into()).compare(&Value::String("abd".into())),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn test_type_coercion() {
        let val = Value::TinyInt(10);
        let bytes = val.serialize(&DataType::Integer).unwrap();
        assert_eq!(bytes, vec![10, 0, 0, 0]);
    }

    #[test]
    fn test_from_conversions() {
        assert_eq!(Value::from(42i32), Value::Integer(42));
        assert_eq!(Value::from("hello"), Value::String("hello".to_string()));
        assert_eq!(Value::from(true), Value::Boolean(true));
    }
}
