use std::cmp::Ordering;

pub trait KeyComparator: Send + Sync {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
}

pub struct IntegerComparator;

impl KeyComparator for IntegerComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() < 4 || b.len() < 4 {
            return a.len().cmp(&b.len());
        }

        let a_val = u32::from_le_bytes([a[0], a[1], a[2], a[3]]);
        let b_val = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);

        a_val.cmp(&b_val)
    }
}

pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}
