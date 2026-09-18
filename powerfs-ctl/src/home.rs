//! Resolve the powerfs-ctl state directory (POWERFS_HOME or ./.powerfs).

use std::path::{Path, PathBuf};

pub struct Home {
    pub root: PathBuf,
}

impl Home {
    /// Resolve from `--home` arg, then POWERFS_HOME env, then default ./.powerfs.
    pub fn resolve(override_home: Option<&str>) -> Self {
        let root = override_home
            .map(PathBuf::from)
            .or_else(|| std::env::var("POWERFS_HOME").ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(".powerfs"));
        Self { root }
    }

    pub fn cluster_toml(&self) -> PathBuf {
        self.root.join("cluster.toml")
    }
    pub fn rendered_dir(&self) -> PathBuf {
        self.root.join("rendered")
    }
    #[allow(dead_code)] // convenience accessor for M2
    pub fn rendered_compose(&self) -> PathBuf {
        self.rendered_dir().join("docker-compose.yml")
    }
    pub fn rendered_config_dir(&self) -> PathBuf {
        self.rendered_dir().join("config")
    }
    pub fn certs_dir(&self) -> PathBuf {
        self.root.join("certs")
    }
    #[allow(dead_code)] // used by M2 (up/down state tracking)
    pub fn state_file(&self) -> PathBuf {
        self.root.join("state.json")
    }

    /// Ensure the state directory skeleton exists.
    pub fn ensure_skeleton(&self) -> std::io::Result<()> {
        for d in [
            &self.root,
            &self.rendered_dir(),
            &self.rendered_config_dir(),
            &self.certs_dir(),
        ] {
            std::fs::create_dir_all(d)?;
        }
        Ok(())
    }

    pub fn load_cluster(&self) -> Result<crate::schema::ClusterConfig, String> {
        let p = self.cluster_toml();
        if !p.exists() {
            return Err(format!(
                "{} not found. Run `powerfs-ctl init` first.",
                p.display()
            ));
        }
        let s = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {}", p.display(), e))?;
        toml::from_str(&s).map_err(|e| format!("parse cluster.toml: {}", e))
    }

    pub fn write_file(&self, rel: &Path, content: &str) -> std::io::Result<()> {
        let full = self.root.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(full, content)
    }
}
