//! Shared helpers for E2E tests that spawn objstrd as a subprocess.
//!
//! Each integration test binary compiles this module independently, so
//! not every symbol is used in every binary.
#![allow(dead_code)]

use std::io::Write;
use std::process::{Child, Command};
use std::time::Duration;

// ---------------------------------------------------------------------------
// ServerProcess
// ---------------------------------------------------------------------------

/// RAII wrapper around a spawned objstrd child process.
/// Kills the child when dropped.
pub struct ServerProcess {
    pub child: Child,
    pub base_url: String,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Binary / port helpers
// ---------------------------------------------------------------------------

/// Path to the compiled objstrd binary (set by cargo test).
pub fn objstrd_bin() -> String {
    std::env::var("CARGO_BIN_EXE_objstrd").unwrap_or_else(|_| "objstrd".to_string())
}

/// Parse the listen port from a tree config that contains
/// `listen=127.0.0.1:<port>`.
pub fn parse_port_from_config(config_text: &str) -> u16 {
    config_text
        .split("listen=127.0.0.1:")
        .nth(1)
        .expect("listen=127.0.0.1:<port> not found in config")
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .expect("failed to parse port number from config")
}

/// Parse the listen port for a specific node from a multi-node config.
pub fn parse_port_for_node(config_text: &str, node: &str) -> u16 {
    config_text
        .split(node)
        .nth(1)
        .unwrap_or_else(|| panic!("node '{}' not found in config", node))
        .split("listen=127.0.0.1:")
        .nth(1)
        .expect("listen= not found after node")
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .expect("failed to parse port number from config")
}

// ---------------------------------------------------------------------------
// Server readiness
// ---------------------------------------------------------------------------

/// Wait for the server to respond to `GET /_admin/info`.
pub async fn wait_for_server(base: &str, client: &reqwest::Client, timeout: Duration) -> bool {
    wait_for_server_with_token(base, client, timeout, None).await
}

/// Wait for the server to respond to `GET /_admin/info`, optionally with
/// a Bearer token.
pub async fn wait_for_server_with_token(
    base: &str,
    client: &reqwest::Client,
    timeout: Duration,
    token: Option<&str>,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        let mut req = client.get(&format!("{base}/_admin/info"));
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        match req.send().await {
            Ok(r) if r.status().is_success() => return true,
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Image formatting
// ---------------------------------------------------------------------------

/// Format a raw image file for use as a shard.
pub fn format_raw_image(path: &std::path::Path, size_mb: u64) {
    let store = rawobjstr::store::RawObjectStore::format_with_size(
        path,
        size_mb * 1024 * 1024,
        false,
    )
    .expect("failed to format raw image");
    let _ = store.flush_index();
    drop(store);
}

// ---------------------------------------------------------------------------
// Config file writers
// ---------------------------------------------------------------------------

/// Write a single-node tree config with one raw shard.
pub fn write_single_config(
    path: &std::path::Path,
    shard_path: &str,
    port: u16,
) {
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, "cluster  test-subproc").unwrap();
    writeln!(f, "bucket   testbucket").unwrap();
    writeln!(f, "").unwrap();
    writeln!(
        f,
        "node1  rf=1  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
        port, port
    )
    .unwrap();
    writeln!(f, "  raw  {}", shard_path).unwrap();
}

/// Write a multi-shard tree config.
pub fn write_multi_config(
    path: &std::path::Path,
    shard_paths: &[&str],
    port: u16,
    rf: usize,
) {
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, "cluster  test-subproc").unwrap();
    writeln!(f, "bucket   testbucket").unwrap();
    writeln!(f, "").unwrap();
    write!(
        f,
        "node1  rf={}  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
        rf, port, port
    )
    .unwrap();
    for shard_path in shard_paths {
        writeln!(f, "").unwrap();
        write!(f, "  raw  {}", shard_path).unwrap();
    }
    writeln!(f, "").unwrap();
}

// ---------------------------------------------------------------------------
// Server spawning
// ---------------------------------------------------------------------------

/// Start an objstrd server with a tree config.
pub fn start_server(config_path: &std::path::Path, node: &str) -> ServerProcess {
    start_server_with_args(config_path, node, &[])
}

/// Start an objstrd server with a tree config and extra CLI args.
pub fn start_server_with_args(
    config_path: &std::path::Path,
    node: &str,
    extra_args: &[&str],
) -> ServerProcess {
    let mut cmd = Command::new(objstrd_bin());
    cmd.args(["--config", config_path.to_str().unwrap(), "--node", node]);
    for arg in extra_args {
        cmd.arg(arg);
    }
    let child = cmd
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn objstrd");

    let config_text = std::fs::read_to_string(config_path).unwrap();
    let port = parse_port_for_node(&config_text, node);

    ServerProcess {
        child,
        base_url: format!("http://127.0.0.1:{port}"),
    }
}

/// Start an objstrd server with explicit CLI flags (no config file).
pub fn start_server_standalone(args: &[&str]) -> ServerProcess {
    let port_str = {
        let mut it = args.iter();
        loop {
            match it.next() {
                Some(&"--port") => break it.next().unwrap().to_string(),
                None => panic!("--port not found in args"),
                _ => {}
            }
        }
    };
    let child = Command::new(objstrd_bin())
        .args(args)
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn objstrd");

    ServerProcess {
        child,
        base_url: format!("http://127.0.0.1:{port_str}"),
    }
}

// ---------------------------------------------------------------------------
// reqwest client
// ---------------------------------------------------------------------------

/// Build an HTTP client with no proxy and a 10s timeout.
pub fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}
