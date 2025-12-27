use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::common::{FrameId, Timestamp};

/// Tracks access history for a single frame
#[derive(Debug)]
struct FrameAccessInfo {
    /// History of access timestamps (most recent at back)
    history: VecDeque<Timestamp>,
    /// Whether this frame is currently evictable
    is_evictable: bool,
}

impl FrameAccessInfo {
    fn new() -> Self {
        Self {
            history: VecDeque::new(),
            is_evictable: false,
        }
    }

    /// Records an access at the given timestamp
    fn record_access(&mut self, timestamp: Timestamp, k: usize) {
        self.history.push_back(timestamp);
        // Keep only the last k accesses
        while self.history.len() > k {
            self.history.pop_front();
        }
    }

    /// Returns the k-distance (backward k-distance from current timestamp)
    /// Returns None if this frame has fewer than k accesses (meaning +inf distance)
    fn k_distance(&self, current_timestamp: Timestamp, k: usize) -> Option<Timestamp> {
        if self.history.len() < k {
            None // +inf distance
        } else {
            // The kth previous access is at index (len - k)
            // k-distance = current_timestamp - timestamp_of_kth_previous_access
            Some(current_timestamp - self.history[self.history.len() - k])
        }
    }

    /// Returns the earliest timestamp in the history
    fn earliest_timestamp(&self) -> Option<Timestamp> {
        self.history.front().copied()
    }
}

/// LRU-K Replacement Policy
///
/// The LRU-K algorithm evicts a frame whose backward k-distance is the maximum
/// of all frames in the replacer. Backward k-distance is computed as the difference
/// in time between the current timestamp and the timestamp of kth previous access.
///
/// A frame with fewer than k historical accesses is given +inf as its backward k-distance.
/// If multiple frames have +inf backward k-distance, the replacer evicts the frame
/// with the earliest overall timestamp.
pub struct LruKReplacer {
    /// K value for the LRU-K algorithm
    k: usize,
    /// Maximum number of frames the replacer can track
    max_frames: usize,
    /// Current timestamp (monotonically increasing)
    current_timestamp: AtomicU64,
    /// Access information for each frame
    frame_info: Mutex<HashMap<FrameId, FrameAccessInfo>>,
    /// Number of evictable frames
    num_evictable: Mutex<usize>,
}

impl LruKReplacer {
    /// Creates a new LRU-K replacer with the given k value and maximum frame count.
    pub fn new(k: usize, max_frames: usize) -> Self {
        Self {
            k,
            max_frames,
            current_timestamp: AtomicU64::new(0),
            frame_info: Mutex::new(HashMap::new()),
            num_evictable: Mutex::new(0),
        }
    }

    /// Evicts the frame with the largest backward k-distance.
    /// Returns None if there are no evictable frames.
    pub fn evict(&self) -> Option<FrameId> {
        let mut frame_info = self.frame_info.lock();
        let mut num_evictable = self.num_evictable.lock();

        if *num_evictable == 0 {
            return None;
        }

        let current_ts = self.current_timestamp.load(Ordering::Relaxed);

        // Find the frame with the largest k-distance
        // Frames with +inf distance (fewer than k accesses) have priority
        // Among +inf frames, pick the one with earliest timestamp

        let mut victim: Option<FrameId> = None;
        let mut victim_k_dist: Option<Timestamp> = None;
        let mut victim_earliest_ts: Option<Timestamp> = None;

        for (frame_id, info) in frame_info.iter() {
            if !info.is_evictable {
                continue;
            }

            let k_dist = info.k_distance(current_ts, self.k);
            let earliest_ts = info.earliest_timestamp();

            let should_replace = match (victim_k_dist, k_dist) {
                // Current victim has +inf, candidate has finite -> don't replace
                (None, Some(_)) => false,
                // Current victim has finite, candidate has +inf -> replace
                (Some(_), None) => true,
                // Both have +inf -> compare earliest timestamps
                (None, None) => match (victim_earliest_ts, earliest_ts) {
                    (Some(v_ts), Some(c_ts)) => c_ts < v_ts,
                    (None, Some(_)) => true,
                    _ => false,
                },
                // Both have finite k-distance -> pick larger one
                (Some(v_dist), Some(c_dist)) => c_dist > v_dist,
            };

            if victim.is_none() || should_replace {
                victim = Some(*frame_id);
                victim_k_dist = k_dist;
                victim_earliest_ts = earliest_ts;
            }
        }

        if let Some(frame_id) = victim {
            frame_info.remove(&frame_id);
            *num_evictable -= 1;
        }

        victim
    }

    /// Records that the given frame was accessed at the current timestamp.
    /// This method should be called after a page is pinned in the BufferPoolManager.
    pub fn record_access(&self, frame_id: FrameId) {
        if frame_id.as_usize() >= self.max_frames {
            return;
        }

        let timestamp = self.current_timestamp.fetch_add(1, Ordering::Relaxed);
        let mut frame_info = self.frame_info.lock();

        frame_info
            .entry(frame_id)
            .or_insert_with(FrameAccessInfo::new)
            .record_access(timestamp, self.k);
    }

    /// Sets whether a frame is evictable.
    /// When a frame's pin count drops to 0, it should be marked as evictable.
    pub fn set_evictable(&self, frame_id: FrameId, is_evictable: bool) {
        if frame_id.as_usize() >= self.max_frames {
            return;
        }

        let mut frame_info = self.frame_info.lock();
        let mut num_evictable = self.num_evictable.lock();

        if let Some(info) = frame_info.get_mut(&frame_id) {
            if info.is_evictable != is_evictable {
                if is_evictable {
                    *num_evictable += 1;
                } else {
                    *num_evictable -= 1;
                }
                info.is_evictable = is_evictable;
            }
        } else if is_evictable {
            // Frame doesn't exist yet but is being marked evictable
            let mut info = FrameAccessInfo::new();
            info.is_evictable = true;
            frame_info.insert(frame_id, info);
            *num_evictable += 1;
        }
    }

    /// Removes a frame from the replacer entirely.
    /// This should be called when a page is deleted from the BufferPoolManager.
    pub fn remove(&self, frame_id: FrameId) {
        let mut frame_info = self.frame_info.lock();
        let mut num_evictable = self.num_evictable.lock();

        if let Some(info) = frame_info.remove(&frame_id) {
            if info.is_evictable {
                *num_evictable -= 1;
            }
        }
    }

    /// Returns the number of evictable frames.
    pub fn size(&self) -> usize {
        *self.num_evictable.lock()
    }

    /// Returns the k value of this replacer.
    pub fn k(&self) -> usize {
        self.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_k_replacer_new() {
        let replacer = LruKReplacer::new(2, 10);
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.k(), 2);
    }

    #[test]
    fn test_lru_k_replacer_evict_empty() {
        let replacer = LruKReplacer::new(2, 10);
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_k_replacer_basic() {
        let replacer = LruKReplacer::new(2, 10);

        // Add frames 0, 1, 2
        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(2));

        // Mark them evictable
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        assert_eq!(replacer.size(), 3);

        // All have only 1 access (less than k=2), so all have +inf distance
        // Should evict the one with earliest timestamp (frame 0)
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.size(), 2);
    }

    #[test]
    fn test_lru_k_replacer_k_distance() {
        let replacer = LruKReplacer::new(2, 10);

        // Access frame 0 twice
        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(0));

        // Access frame 1 once
        replacer.record_access(FrameId::new(1));

        // Mark both evictable
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Frame 0 has k=2 accesses, frame 1 has only 1 (< k)
        // Frame 1 has +inf distance, so it should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_lru_k_replacer_not_evictable() {
        let replacer = LruKReplacer::new(2, 10);

        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));

        // Only mark frame 1 evictable
        replacer.set_evictable(FrameId::new(1), true);

        assert_eq!(replacer.size(), 1);
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_k_replacer_remove() {
        let replacer = LruKReplacer::new(2, 10);

        replacer.record_access(FrameId::new(0));
        replacer.set_evictable(FrameId::new(0), true);

        assert_eq!(replacer.size(), 1);

        replacer.remove(FrameId::new(0));

        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_k_replacer_toggle_evictable() {
        let replacer = LruKReplacer::new(2, 10);

        replacer.record_access(FrameId::new(0));
        replacer.set_evictable(FrameId::new(0), true);
        assert_eq!(replacer.size(), 1);

        replacer.set_evictable(FrameId::new(0), false);
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);

        replacer.set_evictable(FrameId::new(0), true);
        assert_eq!(replacer.size(), 1);
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }

    #[test]
    fn test_lru_k_replacer_largest_k_distance() {
        let replacer = LruKReplacer::new(2, 10);

        // Frame 0: access at t=0, t=1
        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(0));

        // Frame 1: access at t=2, t=3
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(1));

        // Frame 2: access at t=4, t=5
        replacer.record_access(FrameId::new(2));
        replacer.record_access(FrameId::new(2));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // All have k=2 accesses
        // Frame 0 has k-distance = current_ts - 0 = 6
        // Frame 1 has k-distance = current_ts - 2 = 4
        // Frame 2 has k-distance = current_ts - 4 = 2
        // Frame 0 has largest k-distance, should be evicted
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }
}
