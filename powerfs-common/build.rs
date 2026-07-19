//! Build script for powerfs-common.
//!
//! Injects workspace-wide build metadata (git commit/branch, build timestamp,
//! build host, rustc version) as compile-time environment variables so they
//! can be read via `env!` in `build_info.rs`.

use std::process::Command;

fn main() {
    let now = chrono::Utc::now();

    // Build identifier — unique per build invocation. Prefer the value
    // exported by the build wrapper (scripts/build.sh) so CI/deploys can
    // force build.rs to re-run and stamp a fresh id even on incremental
    // builds. Fall back to a timestamp+pid derived id for direct `cargo build`.
    let build_id = std::env::var("POWERFS_BUILD_ID").unwrap_or_else(|_| {
        let ts = now
            .timestamp_nanos_opt()
            .map(|n| n.to_string())
            .unwrap_or_else(|| now.timestamp().to_string());
        format!("{}-{}", ts, std::process::id())
    });
    println!("cargo:rustc-env=POWERFS_BUILD_ID={}", build_id);

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
    println!("cargo:rustc-env=BUILD_TIME={}", now.to_rfc3339());

    // Build host: prefer HOSTNAME (Linux), fall back to COMPUTERNAME (Windows),
    // finally fall back to "unknown".
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_HOST={}", hostname);

    // Rustc version
    let rustc = run_command("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc);

    // Re-run build.rs when POWERFS_BUILD_ID changes (the build wrapper
    // scripts/build.sh exports a fresh value per invocation to force a new
    // build id + timestamp on incremental builds) or when Cargo.toml changes.
    // We intentionally do NOT add `cargo:rerun-if-changed=.git/HEAD` because
    // `.git` may not exist (e.g. in Docker builds without VCS history) and
    // that emits a noisy warning. Direct `cargo build` without the wrapper
    // still gets a fresh id on clean builds via the timestamp fallback above.
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
