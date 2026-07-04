use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};

use super::{RunQuery, RunSummary};

#[derive(Debug, Default)]
struct ObjectStoreReadCounters {
    get_requests: AtomicU64,
    get_ranges_requests: AtomicU64,
    head_requests: AtomicU64,
    list_requests: AtomicU64,
    bytes_read: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObjectStoreReadSnapshot {
    pub(crate) get_requests: u64,
    pub(crate) get_ranges_requests: u64,
    pub(crate) head_requests: u64,
    pub(crate) list_requests: u64,
    pub(crate) bytes_read: u64,
}

impl ObjectStoreReadSnapshot {
    pub(crate) fn request_count(self) -> u64 {
        self.get_requests + self.get_ranges_requests + self.head_requests + self.list_requests
    }
}

pub(crate) fn datafusion_batch_query(query: &RunQuery) -> RunQuery {
    let mut batch_query = query.clone();
    if let Some(limit) = query.limit {
        batch_query.limit = Some(limit.saturating_add(query.offset.unwrap_or(0)));
    }
    batch_query.offset = None;
    batch_query
}

pub(crate) fn page_datafusion_runs(mut runs: Vec<RunSummary>, query: &RunQuery) -> Vec<RunSummary> {
    runs.sort_by(|left, right| {
        let time_order = if query.newest_first {
            right.start_time_unix_nano.cmp(&left.start_time_unix_nano)
        } else {
            left.start_time_unix_nano.cmp(&right.start_time_unix_nano)
        };
        time_order.then(left.span_id.cmp(&right.span_id))
    });

    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(usize::MAX);
    runs.into_iter().skip(offset).take(limit).collect()
}

pub(crate) fn enforce_runtime_object_store_limits(
    query: &RunQuery,
    object_store_reads: ObjectStoreReadSnapshot,
) -> anyhow::Result<()> {
    if let Some(limit) = query.limits.max_estimated_object_store_requests {
        let actual = object_store_reads.request_count();
        if actual > limit as u64 {
            anyhow::bail!(
                "query rejected: actual object-store requests {actual} exceed limit {limit}"
            );
        }
    }

    if let Some(limit) = query.limits.max_candidate_bytes
        && object_store_reads.bytes_read > limit as u64
    {
        let actual = object_store_reads.bytes_read;
        anyhow::bail!("query rejected: actual bytes read {actual} exceed limit {limit}");
    }

    Ok(())
}

impl ObjectStoreReadCounters {
    fn snapshot(&self) -> ObjectStoreReadSnapshot {
        ObjectStoreReadSnapshot {
            get_requests: self.load(&self.get_requests),
            get_ranges_requests: self.load(&self.get_ranges_requests),
            head_requests: self.load(&self.head_requests),
            list_requests: self.load(&self.list_requests),
            bytes_read: self.load(&self.bytes_read),
        }
    }

    fn add(&self, counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    fn load(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MeasuringObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<ObjectStoreReadCounters>,
}

impl MeasuringObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: Arc::new(ObjectStoreReadCounters::default()),
        }
    }

    pub(crate) fn snapshot(&self) -> ObjectStoreReadSnapshot {
        self.counters.snapshot()
    }
}

impl fmt::Display for MeasuringObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MeasuringObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MeasuringObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.counters.add(&self.counters.head_requests, 1);
            return self.inner.get_opts(location, options).await;
        }

        self.counters.add(&self.counters.get_requests, 1);
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let counters = Arc::clone(&self.counters);
        let payload = GetResultPayload::Stream(
            result
                .into_stream()
                .map_ok(move |bytes| {
                    counters.add(&counters.bytes_read, bytes.len() as u64);
                    bytes
                })
                .boxed(),
        );

        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.counters.add(&self.counters.get_ranges_requests, 1);
        let chunks = self.inner.get_ranges(location, ranges).await?;
        self.counters.add(
            &self.counters.bytes_read,
            chunks.iter().map(|chunk| chunk.len() as u64).sum(),
        );
        Ok(chunks)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counters.add(&self.counters.list_requests, 1);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counters.add(&self.counters.list_requests, 1);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counters.add(&self.counters.list_requests, 1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::MeasuringObjectStore;

    #[tokio::test]
    async fn counts_read_requests_and_bytes() -> object_store::Result<()> {
        let store = MeasuringObjectStore::new(Arc::new(InMemory::new()));
        let path = Path::from("query/a.txt");
        store.put(&path, PutPayload::from_static(b"abcdef")).await?;

        store.head(&path).await?;
        assert_eq!(store.get_range(&path, 1..4).await?, b"bcd".as_slice());
        assert_eq!(store.get(&path).await?.bytes().await?, b"abcdef".as_slice());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.head_requests, 1);
        assert_eq!(snapshot.get_requests, 2);
        assert_eq!(snapshot.bytes_read, 9);
        assert_eq!(snapshot.request_count(), 3);
        Ok(())
    }
}
