use std::sync::Arc;

use anyhow::Result;
use kevindb::db::run_migrations;
use kevindb::ingest::IngestConfig as RuntimeIngestConfig;
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
    let ingest_config = RuntimeIngestConfig {
        max_spans_per_segment: config.ingest.max_spans_per_segment,
        max_flush_delay: config.ingest.max_flush_delay,
    };

    let state = ServerState::new(
        config.postgres_url,
        object_store_from_config(config.object_store),
        ingest_config,
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(bind_addr = %config.bind_addr, "kevindb server listening");

    axum::serve(listener, app(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    let flushed = state.flush_pending_ingest().await?;
    if !flushed.is_empty() {
        tracing::info!(
            flushed_segments = flushed.len(),
            "flushed pending ingest during shutdown"
        );
    }

    Ok(())
}

fn object_store_from_config(config: ObjectStoreConfig) -> Arc<dyn ObjectStore> {
    match config {
        ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to listen for shutdown signal");
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to listen for terminate signal");
                }
            }
        };

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}
