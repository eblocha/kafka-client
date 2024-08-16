pub mod clients;
mod cmd;
mod conn;
mod proto;

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use cmd::{admin::AdminCommands, Run};
use conn::manager::ConnectionManager;

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
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let manager = ConnectionManager::new(vec![cli.broker.clone()]);

    let handle = manager.handle(&cli.broker).unwrap();

    let conn = handle
        .get_connection()
        .await
        .ok_or(anyhow!("connection manager is closed"))?;

    match cli.client {
        Client::Admin(cmd) => cmd.run(conn.as_ref()).await?,
    }

    handle.shutdown().await;

    Ok(())
}
