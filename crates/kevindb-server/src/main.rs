use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use kevindb::ingest::{CompactionServiceConfig, IngestConfig as RuntimeIngestConfig};
use kevindb_config::{CacheConfig, CacheMode, ObjectStoreConfig, ServerConfig, ServiceRole};
use kevindb_metastore_postgres::run_migrations;
use kevindb_server::cache::CachedObjectStore;
use kevindb_server::{ServerState, app};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kevindb_server=info,kevindb=info".into()),
        )
        .init();

    let config = ServerConfig::from_env()?;

    if config.run_migrations {
        run_migrations(&config.postgres_url).await?;
    }
    let ingest_config = RuntimeIngestConfig {
        max_spans_per_segment: config.ingest.max_spans_per_segment,
        max_flush_delay: config.ingest.max_flush_delay,
    };

    let service_role = config.service_role;
    let compaction_holder_id = config
        .node_id
        .clone()
        .unwrap_or_else(|| config.bind_addr.to_string());
    let object_store = object_store_from_config(config.object_store, config.cache).await?;
    let state = ServerState::new_with_role(
        config.postgres_url,
        object_store,
        ingest_config,
        config.service_role,
        config.node_id,
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(bind_addr = %config.bind_addr, "kevindb server listening");

    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let compaction_task =
        matches!(service_role, ServiceRole::All | ServiceRole::Compaction).then({
            let state = state.clone();
            let worker_shutdown = shutdown_sender.clone();
            let shutdown_receiver = shutdown_receiver.clone();
            move || {
                tokio::spawn(async move {
                    let result = state
                        .run_compaction_service_loop(
                            CompactionServiceConfig {
                                holder_id: compaction_holder_id,
                                lease_duration: Duration::from_secs(60),
                            },
                            Duration::from_secs(30),
                            shutdown_receiver,
                        )
                        .await;
                    if result.is_err() {
                        let _ = worker_shutdown.send(true);
                    }
                    result
                })
            }
        });

    axum::serve(listener, app(state.clone()))
        .with_graceful_shutdown(shutdown_signal(shutdown_sender, shutdown_receiver))
        .await?;

    if let Some(task) = compaction_task {
        task.await??;
    }

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
        ObjectStoreConfig::S3(config) => {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(config.bucket);
            if let Some(region) = config.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder.build()?;
            Arc::new(PrefixStore::new(store, config.prefix))
        }
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

async fn shutdown_signal(
    shutdown: tokio::sync::watch::Sender<bool>,
    mut shutdown_receiver: tokio::sync::watch::Receiver<bool>,
) {
    tokio::select! {
        () = operating_system_shutdown_signal() => {}
        changed = shutdown_receiver.changed() => {
            if changed.is_err() {
                tracing::warn!("shutdown channel closed unexpectedly");
            }
        }
    }
    let _ = shutdown.send(true);
}

async fn operating_system_shutdown_signal() {
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
