mod cmd;
mod shutdown;

use std::io;

use clap::{Parser, Subcommand};
use cmd::{admin::AdminCommands, Run};
use kafka_client::{common::BrokerHost, config::KafkaConfig};
use tracing::Level;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    client: Client,

    /// A comma-separated list of bootstrap servers to start the client with.
    ///
    /// This does not need to be a list of every address in the cluster, as the client will use one of the servers to
    /// detect all nodes in the cluster.
    ///
    /// If a provided address fails, the next options will be tried.
    #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
    bootstrap_servers: Vec<BrokerHost>,
}

#[derive(Subcommand)]
enum Client {
    /// Perform administrative commands.
    #[command(subcommand)]
    Admin(AdminCommands),
    // /// Produce messages to a topic.
    // #[command(subcommand)]
    // Producer(ProducerCommands),
    // /// Start a consumer and print any record values from the topic to stdout.
    // Consumer {
    //     /// A comma-separated list of topic names to listen on.
    //     #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
    //     topics: Vec<String>,
    // },
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

    let config = KafkaConfig::default();

    match cli.client {
        Client::Admin(cmd) => cmd.run(&cli.bootstrap_servers, config).await?,
    }

    Ok(())
}
