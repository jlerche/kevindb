mod counting_object_store;
mod mockgres;
mod synthetic;
mod workloads;

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use counting_object_store::{CountingObjectStore, ObjectStoreSnapshot};
use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::Serialize;
use workloads::run_core_benchmarks;

#[derive(Debug, Serialize)]
struct BenchSmokeResult {
    workload: &'static str,
    elapsed_nanos: u128,
    listed_objects: usize,
    object_store: ObjectStoreSnapshot,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "smoke".to_owned());
    if mode == "core" {
        let result = run_core_benchmarks().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    let store = CountingObjectStore::new(Arc::new(InMemory::new()));
    let path = Path::from("bench-smoke/payload.bin");
    let started = Instant::now();

    store
        .put(
            &path,
            PutPayload::from_static(b"kevindb bench smoke payload"),
        )
        .await?;
    store.head(&path).await?;
    store.get(&path).await?.bytes().await?;
    let listed_objects = store
        .list(Some(&Path::from("bench-smoke")))
        .try_collect::<Vec<_>>()
        .await?
        .len();

    let result = BenchSmokeResult {
        workload: "object-store-smoke",
        elapsed_nanos: started.elapsed().as_nanos(),
        listed_objects,
        object_store: store.counters().snapshot(),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
