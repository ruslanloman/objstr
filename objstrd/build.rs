use std::process::Command;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let out_dir = std::env::var("OUT_DIR").unwrap_or_default();

    let git_hash = Command::new("git")
        .args(["-C", &manifest_dir, "rev-parse", "--short", "HEAD"])
        .output().ok().filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let git_dirty = Command::new("git")
        .args(["-C", &manifest_dir, "status", "--porcelain"])
        .output().ok().filter(|o| o.status.success())
        .map(|o| if o.stdout.is_empty() { "" } else { "-dirty" })
        .unwrap_or("");
    let build_date = Command::new("date")
        .args(["-u", "+%Y-%m-%d %H:%M:%S UTC"])
        .output().ok().filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let build_info = format!("{}{} {}", git_hash, git_dirty, build_date);

    println!("cargo:rustc-env=BUILD_GIT_HASH={}{}", git_hash, git_dirty);
    println!("cargo:rustc-env=BUILD_DATE={}", build_date);

    // Bake build info into the static HTML pages at compile time.
    for name in &["viz.html", "ui.html", "server.html", "cluster.html", "logs.html", "config.html"] {
        let src = std::path::Path::new(&manifest_dir).join("static").join(name);
        let dst = std::path::Path::new(&out_dir).join(name);
        let content = std::fs::read_to_string(&src)
            .unwrap_or_default()
            .replace("__BUILD_INFO__", &build_info);
        std::fs::write(&dst, content).expect("write generated html");
        println!("cargo:rerun-if-changed=static/{}", name);
    }

    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
    println!("cargo:rerun-if-changed=../git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");
}

