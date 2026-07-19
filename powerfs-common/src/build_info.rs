//! Build-time metadata for PowerFS binaries.
//!
//! Each binary should call `BuildInfo::log_startup()` at startup to emit
//! version/git/build-time information to the log. The workspace-wide fields
//! (git commit, branch, build time, build host, rustc version) are injected by
//! `powerfs-common/build.rs` and are identical across all crates in the
//! workspace. The caller passes its own `CARGO_PKG_VERSION` and
//! `CARGO_PKG_NAME` so the component name is reported correctly.

use serde::Serialize;

/// Workspace-wide build metadata. Construct with [`BuildInfo::current`],
/// passing the calling binary's crate name and version.
#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    /// Component name (e.g. "powerfs-master", "powerfs-fuse").
    pub component: &'static str,
    /// Semantic version from the calling crate's `Cargo.toml`.
    pub version: &'static str,
    /// Short git commit hash at build time. `"unknown"` if not in a git repo.
    pub git_commit: &'static str,
    /// Git branch name at build time.
    pub git_branch: &'static str,
    /// Build timestamp in UTC RFC3339.
    pub build_time: &'static str,
    /// Unique build identifier (timestamp + pid, or the value exported by
    /// the build wrapper via `POWERFS_BUILD_ID`). Lets two binaries produced
    /// by the same commit be distinguished.
    pub build_id: &'static str,
    /// Build host name.
    pub build_host: &'static str,
    /// Rustc version string.
    pub rustc_version: &'static str,
}

impl BuildInfo {
    /// Build a `BuildInfo` for the calling binary.
    ///
    /// Pass `env!("CARGO_PKG_NAME")` and `env!("CARGO_PKG_VERSION")` from the
    /// caller. The workspace-wide fields are read from environment variables
    /// set by `powerfs-common/build.rs` and are the same for every crate in
    /// the workspace.
    pub fn current(component: &'static str, version: &'static str) -> Self {
        BuildInfo {
            component,
            version,
            git_commit: env!("GIT_COMMIT"),
            git_branch: env!("GIT_BRANCH"),
            build_time: env!("BUILD_TIME"),
            build_id: env!("POWERFS_BUILD_ID"),
            build_host: env!("BUILD_HOST"),
            rustc_version: env!("RUSTC_VERSION"),
        }
    }

    /// Emit the build info to the log at INFO level. Call once at startup.
    pub fn log_startup(&self) {
        log::info!("====== PowerFS Build Info ======");
        log::info!("  Component:    {}", self.component);
        log::info!("  Version:      {}", self.version);
        log::info!(
            "  Git Commit:   {} ({})",
            self.git_commit,
            self.git_branch
        );
        log::info!("  Build Time:   {}", self.build_time);
        log::info!("  Build ID:     {}", self.build_id);
        log::info!("  Build Host:   {}", self.build_host);
        log::info!("  Rustc:        {}", self.rustc_version);
        log::info!("================================");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_current_returns_non_unknown_for_rustc() {
        // rustc version is independent of git, so it must be a real value.
        let info = BuildInfo::current("test-crate", "0.0.0-test");
        assert_ne!(info.rustc_version, "unknown");
        assert!(info.rustc_version.contains("rustc"));
    }

    #[test]
    fn build_info_current_preserves_component_and_version() {
        let info = BuildInfo::current("my-component", "1.2.3");
        assert_eq!(info.component, "my-component");
        assert_eq!(info.version, "1.2.3");
    }

    #[test]
    fn build_info_current_has_build_time_in_rfc3339() {
        let info = BuildInfo::current("test-crate", "0.0.0-test");
        // RFC3339 timestamps contain a 'T' separator and a timezone offset
        // (or 'Z' for UTC).
        assert!(
            info.build_time.contains('T'),
            "build_time should be RFC3339, got: {}",
            info.build_time
        );
    }

    #[test]
    fn build_info_current_has_non_empty_build_id() {
        let info = BuildInfo::current("test-crate", "0.0.0-test");
        // build_id is either the value exported by the build wrapper or a
        // timestamp+pid fallback; it must never be empty.
        assert!(
            !info.build_id.is_empty(),
            "build_id should not be empty, got: {:?}",
            info.build_id
        );
    }
}