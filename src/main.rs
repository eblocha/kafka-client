pub mod clients;
mod cmd;
mod conn;
mod proto;

use std::io;

use clap::{Parser, Subcommand};
use clients::network::NetworkClient;
use cmd::{admin::AdminCommands, Run};
use tracing::Level;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    client: Client,

    #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true, help = "bootstrap servers (required)")]
    bootstrap_servers: Vec<String>,
}

#[derive(Subcommand)]
enum Client {
    #[command(subcommand)]
    Admin(AdminCommands),
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_max_level(Level::DEBUG)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let manager = NetworkClient::new(cli.bootstrap_servers, Default::default());

    match cli.client {
        Client::Admin(cmd) => cmd.run(&manager).await?,
    }

    manager.shutdown().await;

    Ok(())
}
