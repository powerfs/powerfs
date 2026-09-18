//! `powerfs-ctl config show` — print rendered output without writing files.

use crate::home::Home;
use crate::render;

pub async fn run(home: &Home) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let resolved = cfg.validate().map_err(|e| e.to_string())?;
    let rendered = render::render(&resolved).map_err(|e| e.to_string())?;

    println!("===== docker-compose.yml =====");
    println!("{}", rendered.compose_yaml);
    for (name, toml) in &rendered.configs {
        println!("\n===== config/{}.toml =====", name);
        println!("{}", toml);
    }
    Ok(())
}
