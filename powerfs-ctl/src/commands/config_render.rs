//! `powerfs-ctl config render` — render cluster.toml into .powerfs/rendered/.

use crate::home::Home;
use crate::render;
use std::path::Path;

pub async fn run(home: &Home) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let resolved = cfg.validate().map_err(|e| e.to_string())?;
    let rendered = render::render(&resolved).map_err(|e| e.to_string())?;

    home.write_file(
        Path::new("rendered/docker-compose.yml"),
        &rendered.compose_yaml,
    )
    .map_err(|e| e.to_string())?;

    let config_dir = home.rendered_config_dir();
    std::fs::create_dir_all(&config_dir).map_err(|e| e.to_string())?;
    for (name, toml) in &rendered.configs {
        let p = config_dir.join(format!("{}.toml", name));
        std::fs::write(&p, toml).map_err(|e| format!("write {}: {}", p.display(), e))?;
    }

    println!("✓ Rendered to {}", home.rendered_dir().display());
    println!("  - docker-compose.yml");
    for name in rendered.configs.keys() {
        println!("  - config/{}.toml", name);
    }
    Ok(())
}
