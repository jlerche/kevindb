use std::sync::Arc;

use anyhow::Result;
use kevindb::db::run_migrations;
use kevindb::ingest::IngestConfig;
use kevindb_config::{ObjectStoreConfig, ServerConfig};
use kevindb_server::{ServerState, app};
use object_store::ObjectStore;
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kevindb_server=info,kevindb=info".into()),
        )
        .init();

    let config = ServerConfig::from_env()?;

    run_migrations(&config.postgres_url).await?;

    let state = ServerState::new(
        config.postgres_url,
        object_store_from_config(config.object_store),
        IngestConfig::default(),
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(bind_addr = %config.bind_addr, "kevindb server listening");

    axum::serve(listener, app(state)).await?;
    Ok(())
}

fn object_store_from_config(config: ObjectStoreConfig) -> Arc<dyn ObjectStore> {
    match config {
        ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
    }
}
