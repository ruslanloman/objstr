/// Jump consistent hash (Lamping & Veach, Google 2014).
///
/// Maps (key, num_buckets) -> bucket in [0, num_buckets) such that when
/// num_buckets changes by 1 only ~1/num_buckets fraction of keys move.
/// O(ln num_buckets) time, zero memory.
///
/// We use this instead of the naive `hash % N` which remaps ~(N-1)/N of
/// all keys when N changes -- a near-total reshuffle.  Jump hash keeps
/// placement stable as shards are added or removed, meaning only the
/// minimum number of objects need to migrate.
///
/// Reference: "A Fast, Minimal Memory, Consistent Hash Algorithm"
/// https://arxiv.org/abs/1406.2294
pub fn jump_consistent_hash(mut key: u64, num_buckets: u32) -> u32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_buckets as i64 {
        b = j;
        key = key.wrapping_mul(2862933555777941757).wrapping_add(1);
        j = ((b + 1) as f64 * ((1i64 << 31) as f64 / ((key >> 33) + 1) as f64)) as i64;
    }
    b as u32
}

/// Controls how objects are placed and replicated across shards.
pub struct ReplicationPolicy {
    replication_factor: usize,
}

impl ReplicationPolicy {
    pub fn new(replication_factor: usize, shard_count: usize) -> Self {
        Self {
            replication_factor: replication_factor.min(shard_count).max(1),
        }
    }

    /// How many copies of each object.
    pub fn factor(&self) -> usize {
        self.replication_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_factor_clamped() {
        let policy = ReplicationPolicy::new(5, 3);
        assert_eq!(policy.factor(), 3); // clamped to shard_count
    }

    #[test]
    fn jump_hash_range_and_determinism() {
        // Every result must be in [0, num_buckets).
        for buckets in 1u32..=64 {
            for seed in 0u64..200 {
                let b = jump_consistent_hash(seed, buckets);
                assert!(b < buckets, "bucket {b} >= {buckets} for seed {seed}");
            }
        }
        // Same inputs always produce the same output.
        assert_eq!(
            jump_consistent_hash(42, 10),
            jump_consistent_hash(42, 10),
        );
    }

    #[test]
    fn jump_hash_stability_on_grow() {
        // When going from N to N+1 buckets, at most ~1/(N+1) keys should
        // change bucket.  With 10000 keys going from 8 to 9 shards the
        // expected move rate is ~11%.  We allow up to 20% to account for
        // statistical noise.
        let n_old = 8u32;
        let n_new = 9u32;
        let samples = 10_000u64;
        let mut moved = 0u64;
        for key in 0..samples {
            if jump_consistent_hash(key, n_old) != jump_consistent_hash(key, n_new) {
                moved += 1;
            }
        }
        let move_pct = (moved as f64 / samples as f64) * 100.0;
        assert!(
            move_pct < 20.0,
            "too many keys moved: {moved}/{samples} ({move_pct:.1}%)"
        );
    }
}
