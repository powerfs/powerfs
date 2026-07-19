//! Build script for powerfs-common.
//!
//! Injects workspace-wide build metadata (git commit/branch, build timestamp,
//! build host, rustc version) as compile-time environment variables so they
//! can be read via `env!` in `build_info.rs`.

use std::process::Command;

fn main() {
    // Git commit hash (short form for readability)
    let commit = run_command("git", &["rev-parse", "--short", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT={}", commit);

    // Git branch name
    let branch = run_command("git", &["rev-parse", "--abbrev-ref", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_BRANCH={}", branch);

    // Build timestamp in UTC RFC3339 (workspace-wide, same for all crates
    // built in the same `cargo build` invocation).
    let now = chrono::Utc::now();
    println!("cargo:rustc-env=BUILD_TIME={}", now.to_rfc3339());

    // Build host: prefer HOSTNAME (Linux), fall back to COMPUTERNAME (Windows),
    // finally fall back to "unknown".
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_HOST={}", hostname);

    // Rustc version
    let rustc = run_command("rustc", &["--version"])
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc);

    // Re-run build.rs only when these env vars change or when Cargo.toml
    // changes. We intentionally do NOT add `cargo:rerun-if-changed=.git/HEAD`
    // because `.git` may not exist (e.g. in Docker builds without VCS history)
    // and that emits a noisy warning. Build info updates on every clean build.
    println!("cargo:rerun-if-env-changed=POWERFS_BUILD_ID");
    println!("cargo:rerun-if-changed=Cargo.toml");
}

fn run_command(cmd: &str, args: &[&str]) -> Option<String> {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}