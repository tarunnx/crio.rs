//! Integration tests for the LRU-K replacer

use crio::buffer::LruKReplacer;
use crio::common::FrameId;

#[test]
fn test_lru_k_eviction_order() {
    let replacer = LruKReplacer::new(2, 10);

    // Access frames in order
    for i in 0..5 {
        replacer.record_access(FrameId::new(i));
        replacer.set_evictable(FrameId::new(i), true);
    }

    assert_eq!(replacer.size(), 5);

    // All frames have only 1 access (< k=2), so all have +inf distance
    // Should evict in order of earliest timestamp (FIFO for +inf frames)
    for i in 0..5 {
        assert_eq!(replacer.evict(), Some(FrameId::new(i)));
    }

    assert_eq!(replacer.size(), 0);
    assert_eq!(replacer.evict(), None);
}

#[test]
fn test_lru_k_respects_k_distance() {
    let replacer = LruKReplacer::new(2, 10);

    // Frame 0: accessed once (will have +inf k-distance)
    replacer.record_access(FrameId::new(0));

    // Frame 1: accessed twice (will have finite k-distance)
    replacer.record_access(FrameId::new(1));
    replacer.record_access(FrameId::new(1));

    // Frame 2: accessed twice (will have finite k-distance, but more recent)
    replacer.record_access(FrameId::new(2));
    replacer.record_access(FrameId::new(2));

    replacer.set_evictable(FrameId::new(0), true);
    replacer.set_evictable(FrameId::new(1), true);
    replacer.set_evictable(FrameId::new(2), true);

    // Frame 0 has +inf k-distance, should be evicted first
    assert_eq!(replacer.evict(), Some(FrameId::new(0)));

    // Between frames 1 and 2, frame 1 has larger k-distance (accessed earlier)
    assert_eq!(replacer.evict(), Some(FrameId::new(1)));

    // Frame 2 is last
    assert_eq!(replacer.evict(), Some(FrameId::new(2)));
}

#[test]
fn test_lru_k_pinned_frames_not_evicted() {
    let replacer = LruKReplacer::new(2, 10);

    replacer.record_access(FrameId::new(0));
    replacer.record_access(FrameId::new(1));
    replacer.record_access(FrameId::new(2));

    // Only mark frames 1 and 2 as evictable
    replacer.set_evictable(FrameId::new(1), true);
    replacer.set_evictable(FrameId::new(2), true);

    assert_eq!(replacer.size(), 2);

    // Frame 0 should never be evicted
    assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    assert_eq!(replacer.evict(), None);
}

#[test]
fn test_lru_k_toggle_evictable() {
    let replacer = LruKReplacer::new(2, 10);

    replacer.record_access(FrameId::new(0));
    replacer.set_evictable(FrameId::new(0), true);
    assert_eq!(replacer.size(), 1);

    // Toggle off
    replacer.set_evictable(FrameId::new(0), false);
    assert_eq!(replacer.size(), 0);
    assert_eq!(replacer.evict(), None);

    // Toggle back on
    replacer.set_evictable(FrameId::new(0), true);
    assert_eq!(replacer.size(), 1);
    assert_eq!(replacer.evict(), Some(FrameId::new(0)));
}

#[test]
fn test_lru_k_remove() {
    let replacer = LruKReplacer::new(2, 10);

    replacer.record_access(FrameId::new(0));
    replacer.record_access(FrameId::new(1));
    replacer.set_evictable(FrameId::new(0), true);
    replacer.set_evictable(FrameId::new(1), true);

    assert_eq!(replacer.size(), 2);

    // Remove frame 0
    replacer.remove(FrameId::new(0));
    assert_eq!(replacer.size(), 1);

    // Only frame 1 should be evictable
    assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    assert_eq!(replacer.evict(), None);
}

#[test]
fn test_lru_k_multiple_inf_distance() {
    let replacer = LruKReplacer::new(3, 10);

    // All frames have fewer than k=3 accesses
    replacer.record_access(FrameId::new(0));

    replacer.record_access(FrameId::new(1));
    replacer.record_access(FrameId::new(1));

    replacer.record_access(FrameId::new(2));

    for i in 0..3 {
        replacer.set_evictable(FrameId::new(i), true);
    }

    // All have +inf k-distance, so evict by earliest timestamp
    // Frame 0 was accessed first
    assert_eq!(replacer.evict(), Some(FrameId::new(0)));

    // Frame 1's first access was second overall
    assert_eq!(replacer.evict(), Some(FrameId::new(1)));

    // Frame 2 was accessed last
    assert_eq!(replacer.evict(), Some(FrameId::new(2)));
}

#[test]
fn test_lru_k_history_limit() {
    let replacer = LruKReplacer::new(2, 10);

    // Access frame 0 many times
    for _ in 0..10 {
        replacer.record_access(FrameId::new(0));
    }

    // Access frame 1 twice
    replacer.record_access(FrameId::new(1));
    replacer.record_access(FrameId::new(1));

    replacer.set_evictable(FrameId::new(0), true);
    replacer.set_evictable(FrameId::new(1), true);

    // Frame 0's k-distance is based on its most recent k accesses
    // Frame 1 was accessed more recently, so frame 0 should have larger k-distance
    assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    assert_eq!(replacer.evict(), Some(FrameId::new(1)));
}

#[test]
fn test_lru_k_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let replacer = Arc::new(LruKReplacer::new(2, 100));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let replacer = Arc::clone(&replacer);
            thread::spawn(move || {
                for i in 0..25 {
                    let frame_id = FrameId::new((t * 25 + i) as u32);
                    replacer.record_access(frame_id);
                    replacer.set_evictable(frame_id, true);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(replacer.size(), 100);

    // Evict all frames
    for _ in 0..100 {
        assert!(replacer.evict().is_some());
    }

    assert_eq!(replacer.size(), 0);
}
