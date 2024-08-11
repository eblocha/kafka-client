pub mod clients;
mod cmd;
mod conn;
mod manager;
mod proto;

use clap::{Parser, Subcommand};
use cmd::{admin::AdminCommands, Run};
use conn::KafkaConnectionConfig;

use manager::version::VersionedConnection;
use tokio::net::TcpStream;

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
    let cli = Cli::parse();

    let tcp = TcpStream::connect(&cli.broker).await?;

    let conn = VersionedConnection::connect(tcp, &KafkaConnectionConfig::default()).await?;

    match cli.client {
        Client::Admin(cmd) => cmd.run(&conn).await?,
    }

    conn.shutdown().await;

    Ok(())
}
