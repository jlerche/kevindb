use std::sync::Arc;

use anyhow::Result;
use kevindb::db::run_migrations;
use kevindb::ingest::IngestConfig as RuntimeIngestConfig;
use kevindb_config::{CacheConfig, CacheMode, ObjectStoreConfig, ServerConfig};
use kevindb_server::cache::CachedObjectStore;
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

    let object_store = object_store_from_config(config.object_store, config.cache).await?;
    let state = ServerState::new_with_node_id(
        config.postgres_url,
        object_store,
        ingest_config,
        config.node_id,
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

async fn object_store_from_config(
    object_store_config: ObjectStoreConfig,
    cache_config: CacheConfig,
) -> Result<Arc<dyn ObjectStore>> {
    let object_store: Arc<dyn ObjectStore> = match object_store_config {
        ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
    };

    match cache_config.mode {
        CacheMode::Memory => Ok(Arc::new(CachedObjectStore::memory(
            object_store,
            cache_config.memory_capacity_bytes,
        ))),
        CacheMode::Hybrid => {
            let cache_dir = cache_config
                .hybrid_dir
                .expect("hybrid cache config requires a directory");
            Ok(Arc::new(
                CachedObjectStore::hybrid(
                    object_store,
                    cache_config.memory_capacity_bytes,
                    cache_dir,
                    cache_config.disk_capacity_bytes,
                    cache_config.disk_block_bytes,
                )
                .await?,
            ))
        }
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
