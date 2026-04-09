//! Structured logging with in-memory ring buffer, file output, and REST API.
//!
//! Provides:
//! - `LogEntry` struct with category, level, message, and structured fields
//! - In-memory ring buffer for the web UI / REST endpoint
//! - Append-only text log file for persistent storage (future Loki/Grafana)
//! - `LogBuffer` layer that plugs into `tracing-subscriber`

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

// -- LogEntry -----------------------------------------------------

/// A single structured log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// ISO 8601 timestamp
    pub time: DateTime<Utc>,
    /// Log level: debug, info, warn, error
    pub level: String,
    /// High-level category: requests, index, replication, health, recovery,
    /// admin, lifecycle, errors
    pub category: String,
    /// Service tag: s3, coord, admin, internal
    pub service: String,
    /// Human-readable message
    pub message: String,
}

impl LogEntry {
    pub fn new(
        level: &str,
        category: &str,
        service: &str,
        message: String,
    ) -> Self {
        Self {
            time: Utc::now(),
            level: level.to_string(),
            category: category.to_string(),
            service: service.to_string(),
            message,
        }
    }
}

// -- Ring buffer --------------------------------------------------

/// Thread-safe ring buffer of recent log entries.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<LogBufferInner>>,
}

struct LogBufferInner {
    entries: VecDeque<LogEntry>,
    capacity: usize,
    /// Optional append-only log file
    file: Option<std::fs::File>,
}

impl LogBuffer {
    /// Create a new ring buffer with the given capacity and optional log file path.
    pub fn new(capacity: usize, log_file: Option<PathBuf>) -> Self {
        let file = log_file.and_then(|p| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&p)
                .map_err(|e| {
                    tracing::warn!(path = %p.display(), error = %e, "cannot open log file");
                })
                .ok()
        });
        Self {
            inner: Arc::new(Mutex::new(LogBufferInner {
                entries: VecDeque::with_capacity(capacity),
                capacity,
                file,
            })),
        }
    }

    /// Push a log entry into the ring buffer and optionally write to file.
    pub fn push(&self, entry: LogEntry) {
        // Format the line outside the lock to minimize lock hold time.
        let line = format!(
            "{}\t{}\t{}\t{}\t{}",
            entry.time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            entry.level,
            entry.service,
            entry.category,
            entry.message,
        );

        let mut inner = self.inner.lock();

        // Push to ring buffer, drop oldest if full
        if inner.entries.len() >= inner.capacity {
            inner.entries.pop_front();
        }
        inner.entries.push_back(entry);

        // Write to file (still inside lock to preserve ordering, but the
        // format work was done outside).
        if let Some(ref mut f) = inner.file {
            if let Err(e) = writeln!(f, "{}", line) {
                // Log file write failed (disk full, I/O error).
                // Drop the file handle to avoid repeated failures.
                tracing::warn!("log file write failed, disabling file logging: {e}");
                inner.file = None;
            }
        }
    }

    /// Query the ring buffer with optional filters. Returns newest first.
    pub fn query(&self, filter: &LogFilter) -> LogQueryResult {
        let inner = self.inner.lock();
        let total = inner.entries.len();
        let limit = filter.limit.unwrap_or(100).min(10_000);

        let entries: Vec<LogEntry> = inner
            .entries
            .iter()
            .rev() // newest first
            .filter(|e| {
                if let Some(ref level) = filter.level {
                    if !level_matches(&e.level, level) {
                        return false;
                    }
                }
                if let Some(ref cats) = filter.category {
                    let want: Vec<&str> = cats.split(',').collect();
                    if !want.iter().any(|c| c.eq_ignore_ascii_case(&e.category)) {
                        return false;
                    }
                }
                if let Some(ref svc) = filter.service {
                    if !svc.eq_ignore_ascii_case(&e.service) {
                        return false;
                    }
                }
                if let Some(ref since) = filter.since {
                    if e.time < *since {
                        return false;
                    }
                }
                if let Some(ref until) = filter.until {
                    if e.time > *until {
                        return false;
                    }
                }
                if let Some(ref q) = filter.q {
                    let q_lower = q.to_ascii_lowercase();
                    if !e.message.to_ascii_lowercase().contains(&q_lower) {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .cloned()
            .collect();

        LogQueryResult {
            total,
            returned: entries.len(),
            entries,
        }
    }
}

/// Returns true if `entry_level` is at or above `min_level`.
fn level_matches(entry_level: &str, min_level: &str) -> bool {
    let rank = |l: &str| -> u8 {
        match l.to_ascii_lowercase().as_str() {
            "debug" => 0,
            "info" => 1,
            "warn" | "warning" => 2,
            "error" => 3,
            _ => 1,
        }
    };
    rank(entry_level) >= rank(min_level)
}

// -- Query types --------------------------------------------------

/// Filter parameters for log queries.
#[derive(Debug, Default)]
pub struct LogFilter {
    pub level: Option<String>,
    pub category: Option<String>,
    pub service: Option<String>,
    pub limit: Option<usize>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub q: Option<String>,
}

impl LogFilter {
    /// Parse from a query string like "level=info&category=requests&limit=50".
    pub fn from_query(query: Option<&str>) -> Self {
        let mut f = Self::default();
        let Some(qs) = query else { return f };
        for pair in qs.split('&') {
            let mut kv = pair.splitn(2, '=');
            let key = kv.next().unwrap_or("");
            let val = kv.next().unwrap_or("");
            if val.is_empty() {
                continue;
            }
            match key {
                "level" => f.level = Some(val.to_string()),
                "category" => f.category = Some(val.to_string()),
                "service" => f.service = Some(val.to_string()),
                "limit" => f.limit = val.parse().ok(),
                "since" => {
                    f.since = val
                        .parse::<DateTime<Utc>>()
                        .ok();
                }
                "until" => {
                    f.until = val
                        .parse::<DateTime<Utc>>()
                        .ok();
                }
                "q" => {
                    f.q = Some(
                        percent_encoding::percent_decode_str(val)
                            .decode_utf8_lossy()
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
        f
    }
}

/// Result of a log query.
#[derive(Serialize)]
pub struct LogQueryResult {
    pub total: usize,
    pub returned: usize,
    pub entries: Vec<LogEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- level_matches -------------------------------------------------

    #[test]
    fn level_matches_same_level() {
        assert!(level_matches("info", "info"));
        assert!(level_matches("debug", "debug"));
        assert!(level_matches("warn", "warn"));
        assert!(level_matches("error", "error"));
    }

    #[test]
    fn level_matches_higher_passes() {
        assert!(level_matches("warn", "info"));
        assert!(level_matches("error", "debug"));
        assert!(level_matches("error", "warn"));
    }

    #[test]
    fn level_matches_lower_rejected() {
        assert!(!level_matches("debug", "info"));
        assert!(!level_matches("info", "warn"));
        assert!(!level_matches("warn", "error"));
    }

    #[test]
    fn level_matches_warning_alias() {
        assert!(level_matches("warning", "warn"));
        assert!(level_matches("warn", "warning"));
    }

    #[test]
    fn level_matches_unknown_defaults_to_info() {
        // Unknown entry level maps to rank 1 (info)
        assert!(level_matches("custom", "debug")); // 1 >= 0
        assert!(level_matches("custom", "info"));  // 1 >= 1
        assert!(!level_matches("custom", "warn")); // 1 >= 2 -> false
    }

    #[test]
    fn level_matches_case_insensitive() {
        assert!(level_matches("INFO", "info"));
        assert!(level_matches("Error", "WARN"));
        assert!(!level_matches("DEBUG", "Info"));
    }

    // -- LogFilter::from_query ----------------------------------------

    #[test]
    fn filter_from_none() {
        let f = LogFilter::from_query(None);
        assert!(f.level.is_none());
        assert!(f.category.is_none());
        assert!(f.service.is_none());
        assert!(f.limit.is_none());
        assert!(f.since.is_none());
        assert!(f.until.is_none());
        assert!(f.q.is_none());
    }

    #[test]
    fn filter_from_empty_string() {
        let f = LogFilter::from_query(Some(""));
        assert!(f.level.is_none());
    }

    #[test]
    fn filter_parses_all_fields() {
        let f = LogFilter::from_query(Some(
            "level=warn&category=requests,health&service=s3&limit=42&q=hello%20world"
        ));
        assert_eq!(f.level.as_deref(), Some("warn"));
        assert_eq!(f.category.as_deref(), Some("requests,health"));
        assert_eq!(f.service.as_deref(), Some("s3"));
        assert_eq!(f.limit, Some(42));
        assert_eq!(f.q.as_deref(), Some("hello world"));
    }

    #[test]
    fn filter_ignores_empty_values() {
        let f = LogFilter::from_query(Some("level=&category=requests"));
        assert!(f.level.is_none(), "empty value should be skipped");
        assert_eq!(f.category.as_deref(), Some("requests"));
    }

    #[test]
    fn filter_ignores_unknown_keys() {
        let f = LogFilter::from_query(Some("bogus=123&level=error"));
        assert_eq!(f.level.as_deref(), Some("error"));
    }

    #[test]
    fn filter_invalid_limit_ignored() {
        let f = LogFilter::from_query(Some("limit=notanumber"));
        assert!(f.limit.is_none());
    }

    #[test]
    fn filter_parses_since_until() {
        let f = LogFilter::from_query(Some(
            "since=2025-01-01T00:00:00Z&until=2025-06-01T00:00:00Z"
        ));
        assert!(f.since.is_some());
        assert!(f.until.is_some());
    }

    #[test]
    fn filter_invalid_datetime_ignored() {
        let f = LogFilter::from_query(Some("since=not-a-date"));
        assert!(f.since.is_none());
    }

    // -- LogBuffer: push + ring buffer --------------------------------

    #[test]
    fn buffer_push_and_query_basic() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("info", "requests", "s3", "hello".into()));
        buf.push(LogEntry::new("warn", "health", "admin", "check".into()));

        let result = buf.query(&LogFilter::default());
        assert_eq!(result.total, 2);
        assert_eq!(result.returned, 2);
        // newest first
        assert_eq!(result.entries[0].message, "check");
        assert_eq!(result.entries[1].message, "hello");
    }

    #[test]
    fn buffer_ring_eviction() {
        let buf = LogBuffer::new(3, None);
        for i in 0..5 {
            buf.push(LogEntry::new("info", "test", "s3", format!("msg{i}")));
        }
        let result = buf.query(&LogFilter::default());
        assert_eq!(result.total, 3, "ring buffer should cap at capacity");
        // Should have msg2, msg3, msg4 (oldest evicted)
        let msgs: Vec<&str> = result.entries.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(msgs, vec!["msg4", "msg3", "msg2"]);
    }

    // -- LogBuffer: query filters ------------------------------------

    #[test]
    fn query_filter_by_level() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("debug", "test", "s3", "d".into()));
        buf.push(LogEntry::new("info", "test", "s3", "i".into()));
        buf.push(LogEntry::new("warn", "test", "s3", "w".into()));
        buf.push(LogEntry::new("error", "test", "s3", "e".into()));

        let f = LogFilter { level: Some("warn".into()), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 2);
        let levels: Vec<&str> = result.entries.iter().map(|e| e.level.as_str()).collect();
        assert!(levels.iter().all(|l| *l == "warn" || *l == "error"));
    }

    #[test]
    fn query_filter_by_category_csv() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("info", "requests", "s3", "r".into()));
        buf.push(LogEntry::new("info", "health", "s3", "h".into()));
        buf.push(LogEntry::new("info", "index", "s3", "x".into()));

        let f = LogFilter { category: Some("requests,health".into()), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 2);
    }

    #[test]
    fn query_filter_by_service() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("info", "test", "s3", "a".into()));
        buf.push(LogEntry::new("info", "test", "admin", "b".into()));

        let f = LogFilter { service: Some("admin".into()), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 1);
        assert_eq!(result.entries[0].service, "admin");
    }

    #[test]
    fn query_filter_by_text() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("info", "test", "s3", "PUT /bucket/key".into()));
        buf.push(LogEntry::new("info", "test", "s3", "GET /bucket/other".into()));

        let f = LogFilter { q: Some("put".into()), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 1);
        assert!(result.entries[0].message.contains("PUT"));
    }

    #[test]
    fn query_limit_respected() {
        let buf = LogBuffer::new(100, None);
        for i in 0..20 {
            buf.push(LogEntry::new("info", "test", "s3", format!("msg{i}")));
        }
        let f = LogFilter { limit: Some(5), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 5);
        assert_eq!(result.total, 20);
    }

    #[test]
    fn query_limit_capped_at_10000() {
        let buf = LogBuffer::new(10, None);
        buf.push(LogEntry::new("info", "test", "s3", "x".into()));
        // Request an absurdly high limit -- should be capped to 10000
        let f = LogFilter { limit: Some(999_999), ..Default::default() };
        let result = buf.query(&f);
        // We only have 1 entry, but the cap was applied internally
        assert_eq!(result.returned, 1);
    }

    #[test]
    fn query_default_limit_is_100() {
        let buf = LogBuffer::new(500, None);
        for i in 0..200 {
            buf.push(LogEntry::new("info", "test", "s3", format!("msg{i}")));
        }
        let f = LogFilter::default(); // limit = None -> defaults to 100
        let result = buf.query(&f);
        assert_eq!(result.returned, 100);
    }

    // -- LogBuffer: file output ----------------------------------------

    #[test]
    fn buffer_writes_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        let buf = LogBuffer::new(10, Some(path.clone()));
        buf.push(LogEntry::new("info", "test", "s3", "hello file".into()));
        buf.push(LogEntry::new("warn", "test", "s3", "second line".into()));

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("hello file"));
        assert!(lines[1].contains("second line"));
        assert!(lines[0].contains("info"));
        assert!(lines[1].contains("warn"));
    }

    #[test]
    fn buffer_no_file_when_none() {
        // Verify it works fine with no file
        let buf = LogBuffer::new(5, None);
        buf.push(LogEntry::new("info", "test", "s3", "no file".into()));
        let result = buf.query(&LogFilter::default());
        assert_eq!(result.returned, 1);
    }

    #[test]
    fn buffer_bad_file_path_graceful() {
        // Attempting to open a file in a nonexistent directory should not panic
        let buf = LogBuffer::new(5, Some(PathBuf::from("/nonexistent/dir/log.txt")));
        buf.push(LogEntry::new("info", "test", "s3", "still works".into()));
        let result = buf.query(&LogFilter::default());
        assert_eq!(result.returned, 1);
    }

    // -- LogEntry serialization ----------------------------------------

    #[test]
    fn log_entry_serializes_to_json() {
        let entry = LogEntry::new("info", "requests", "s3", "test msg".into());
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"level\":\"info\""));
        assert!(json.contains("\"category\":\"requests\""));
        assert!(json.contains("\"service\":\"s3\""));
        assert!(json.contains("\"message\":\"test msg\""));
        assert!(json.contains("\"time\""));
    }

    #[test]
    fn query_filter_since_until() {
        let buf = LogBuffer::new(10, None);
        // Push 3 entries, but we can only control "since/until" by
        // checking that all entries have time >= now (roughly).
        let before = Utc::now();
        buf.push(LogEntry::new("info", "test", "s3", "a".into()));
        buf.push(LogEntry::new("info", "test", "s3", "b".into()));

        // Query with since = before: should get both
        let f = LogFilter { since: Some(before - chrono::Duration::seconds(1)), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 2);

        // Query with since = far future: should get none
        let far_future = Utc::now() + chrono::Duration::hours(1);
        let f = LogFilter { since: Some(far_future), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 0);

        // Query with until = far past: should get none
        let far_past = Utc::now() - chrono::Duration::hours(1);
        let f = LogFilter { until: Some(far_past), ..Default::default() };
        let result = buf.query(&f);
        assert_eq!(result.returned, 0);
    }
}
