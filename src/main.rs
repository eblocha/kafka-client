pub mod clients;
mod cmd;
mod conn;
mod proto;

use std::io;

use clap::{Parser, Subcommand};
use clients::network::NetworkClient;
use cmd::{admin::AdminCommands, Run};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    client: Client,

    #[arg(short, long)]
    broker: String,
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
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let manager = NetworkClient::new(vec![cli.broker.clone()], Default::default());

    match cli.client {
        Client::Admin(cmd) => cmd.run(&manager).await?,
    }

    Ok(())
}
