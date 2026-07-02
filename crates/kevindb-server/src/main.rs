use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use kevindb::db::run_migrations;
use kevindb::ingest::IngestConfig;
use kevindb_server::{ServerState, app};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kevindb_server=info,kevindb=info".into()),
        )
        .init();

    let postgres_url =
        std::env::var("KEVINDB_POSTGRES_URL").context("KEVINDB_POSTGRES_URL must be set")?;
    let bind_addr = std::env::var("KEVINDB_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_owned())
        .parse::<SocketAddr>()
        .context("KEVINDB_BIND_ADDR must be a socket address")?;

    run_migrations(&postgres_url).await?;

    let state = ServerState::new(
        postgres_url,
        Arc::new(InMemory::new()),
        IngestConfig::default(),
    );
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "kevindb server listening");

    axum::serve(listener, app(state)).await?;
    Ok(())
}
