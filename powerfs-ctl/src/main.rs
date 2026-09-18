use clap::Parser;

mod cert;
mod cli;
mod commands;
mod compose;
mod health;
mod home;
mod render;
mod schema;

#[tokio::main]
async fn main() {
    let cli = cli::Cli::parse();
    let home = home::Home::resolve(cli.home.as_deref());
    if let Err(e) = commands::dispatch(cli.command, &home).await {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}
