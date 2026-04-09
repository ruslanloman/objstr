//! E2E tests for admin HTML pages and CORS headers.
//!
//! Verifies that all HTML page endpoints return 200 with text/html
//! content type, and that CORS headers appear when --cors-origin is set.
//!
//! Uses subprocess pattern (spawns real objstrd binary).

mod subprocess_helpers;

use std::time::Duration;
use subprocess_helpers::*;

use std::process::Command;

fn start_server_with_cors(
    config_path: &std::path::Path,
    node: &str,
    cors_origin: &str,
) -> ServerProcess {
    let child = Command::new(objstrd_bin())
        .args(["--config", config_path.to_str().unwrap(), "--node", node])
        .env("RUST_LOG", "info")
        .env("ADMIN_CORS_ORIGIN", cors_origin)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn objstrd");

    let config_text = std::fs::read_to_string(config_path).unwrap();
    let port = parse_port_from_config(&config_text);

    ServerProcess {
        child,
        base_url: format!("http://127.0.0.1:{port}"),
    }
}

// =========================================================================
// HTML page tests
// =========================================================================

/// All known HTML page endpoints should return 200 with text/html.
#[tokio::test]
async fn test_html_pages_return_200() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19200);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let html_paths = [
        "/_admin/",
        "/_admin/viz",
        "/_admin/ui",
        "/_admin/server",
        "/_admin/cluster",
        "/_admin/config",
        "/_admin/logs/ui",
    ];

    for path in &html_paths {
        let url = format!("{}{}", srv.base_url, path);
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(
            resp.status().as_u16(),
            200,
            "expected 200 for {path}, got {}",
            resp.status()
        );
        let ct = resp
            .headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("");
        assert!(
            ct.contains("text/html"),
            "expected text/html for {path}, got {ct}"
        );
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("<html") || body.contains("<!DOCTYPE") || body.contains("<!doctype"),
            "response body for {path} does not look like HTML"
        );
    }

    let _ = srv.child.kill();
}

/// Per-shard HTML pages should return 200 when shard 0 exists.
#[tokio::test]
async fn test_per_shard_html_pages() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19201);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let shard_html_paths = [
        "/_admin/shard/0/viz",
        "/_admin/shard/0/ui",
        "/_admin/shard/0/server",
    ];

    for path in &shard_html_paths {
        let url = format!("{}{}", srv.base_url, path);
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(
            resp.status().as_u16(),
            200,
            "expected 200 for {path}, got {}",
            resp.status()
        );
    }

    // Invalid shard ID should return 404
    let resp = client
        .get(&format!("{}/_admin/shard/99/viz", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    let _ = srv.child.kill();
}

/// Unknown /_admin/* paths should return 404.
#[tokio::test]
async fn test_unknown_admin_path_404() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19202);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/nonexistent", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    let _ = srv.child.kill();
}

// =========================================================================
// CORS tests
// =========================================================================

/// With ADMIN_CORS_ORIGIN set, JSON admin responses should include
/// Access-Control-Allow-Origin header.
#[tokio::test]
async fn test_cors_headers_on_json_endpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19203);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server_with_cors(&config, "node1", "http://example.com");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // JSON endpoint should have CORS header
    let resp = client
        .get(&format!("{}/_admin/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let acao = resp
        .headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert_eq!(acao, "http://example.com");

    let _ = srv.child.kill();
}

/// OPTIONS preflight to /_admin/* should return 200 with CORS headers.
#[tokio::test]
async fn test_cors_preflight_options() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19204);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server_with_cors(&config, "node1", "http://example.com");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .request(
            reqwest::Method::OPTIONS,
            &format!("{}/_admin/info", srv.base_url),
        )
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();

    // CORS preflight returns 204 No Content per the HTTP spec.
    assert_eq!(resp.status().as_u16(), 204);
    let acao = resp
        .headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert_eq!(acao, "http://example.com");

    let _ = srv.child.kill();
}

/// Without ADMIN_CORS_ORIGIN, no CORS headers should appear.
#[tokio::test]
async fn test_no_cors_headers_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19205);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert!(resp.headers().get("access-control-allow-origin").is_none());

    let _ = srv.child.kill();
}
