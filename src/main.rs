mod backoff;
pub mod clients;
mod cmd;
pub mod config;
mod conn;
mod proto;

use std::{io, path::PathBuf};

use clap::{Parser, Subcommand};
use clients::network::NetworkClient;
use cmd::{admin::AdminCommands, producer::produce_from_file, Run};
use config::KafkaConfig;
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
    Producer {
        #[arg(short, long)]
        file: PathBuf,
        #[arg(short, long)]
        topic: String,
    },
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_max_level(Level::INFO)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let cfg = KafkaConfig::default();

    let manager = NetworkClient::try_new(&cli.bootstrap_servers, (&cfg).into()).await?;

    match cli.client {
        Client::Admin(cmd) => cmd.run(&manager).await?,
        Client::Producer { file, topic } => produce_from_file(&manager, topic, file).await?,
    }

    manager.shutdown().await;

    Ok(())
}
