mod cmd;

use std::{io, path::PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use cmd::{admin::AdminCommands, consumer::EchoTopics, Run};
use kafka_client::{clients::network::NetworkClient, common::try_parse_hosts, config::KafkaConfig};
use tracing::Level;
use tracing_subscriber::EnvFilter;

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
    Consumer {
        #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
        topics: Vec<String>,
    },
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_max_level(Level::WARN)
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let cfg = KafkaConfig::default();

    let manager = NetworkClient::try_new(&try_parse_hosts(&cli.bootstrap_servers)?, (&cfg).into())
        .await
        .context("failed to bootstrap client")?;

    match cli.client {
        Client::Admin(cmd) => cmd.run(manager).await?,
        Client::Producer { file, topic } => todo!(),
        Client::Consumer { topics } => EchoTopics { topics }.run(manager).await?,
    }

    Ok(())
}
