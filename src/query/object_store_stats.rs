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
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObjectStoreReadLimits {
    max_requests: Option<u64>,
    max_bytes_read: Option<u64>,
}

impl ObjectStoreReadSnapshot {
    pub(crate) fn request_count(self) -> u64 {
        self.get_requests + self.get_ranges_requests + self.head_requests + self.list_requests
    }
}

impl ObjectStoreReadLimits {
    pub(crate) fn from_query(query: &RunQuery) -> Self {
        Self {
            max_requests: query
                .limits
                .max_estimated_object_store_requests
                .map(|limit| limit as u64),
            max_bytes_read: query
                .limits
                .max_candidate_bytes
                .map(|limit| limit.max(0) as u64),
        }
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

    fn add_request(&self, counter: &AtomicU64, limits: ObjectStoreReadLimits) -> Result<()> {
        counter.fetch_add(1, Ordering::Relaxed);
        let actual = self.snapshot().request_count();
        if let Some(limit) = limits.max_requests
            && actual > limit
        {
            return Err(limit_error(format!(
                "actual object-store requests {actual} exceed limit {limit}"
            )));
        }
        Ok(())
    }

    fn add_bytes_read(&self, amount: u64, limits: ObjectStoreReadLimits) -> Result<()> {
        let actual = self
            .bytes_read
            .fetch_add(amount, Ordering::Relaxed)
            .saturating_add(amount);
        if let Some(limit) = limits.max_bytes_read
            && actual > limit
        {
            return Err(limit_error(format!(
                "actual bytes read {actual} exceed limit {limit}"
            )));
        }
        Ok(())
    }

    fn load(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MeasuringObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<ObjectStoreReadCounters>,
    limits: ObjectStoreReadLimits,
}

impl MeasuringObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self::with_limits(inner, ObjectStoreReadLimits::default())
    }

    pub(crate) fn with_limits(inner: Arc<dyn ObjectStore>, limits: ObjectStoreReadLimits) -> Self {
        Self {
            inner,
            counters: Arc::new(ObjectStoreReadCounters::default()),
            limits,
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
            self.counters
                .add_request(&self.counters.head_requests, self.limits)?;
            return self.inner.get_opts(location, options).await;
        }

        self.counters
            .add_request(&self.counters.get_requests, self.limits)?;
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let counters = Arc::clone(&self.counters);
        let limits = self.limits;
        let payload = GetResultPayload::Stream(
            result
                .into_stream()
                .and_then(move |bytes| {
                    let counters = Arc::clone(&counters);
                    async move {
                        counters.add_bytes_read(bytes.len() as u64, limits)?;
                        Ok(bytes)
                    }
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
        self.counters
            .add_request(&self.counters.get_ranges_requests, self.limits)?;
        let chunks = self.inner.get_ranges(location, ranges).await?;
        self.counters.add_bytes_read(
            chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            self.limits,
        )?;
        Ok(chunks)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let result = self
            .counters
            .add_request(&self.counters.list_requests, self.limits);
        if let Err(error) = result {
            return futures_util::stream::once(async { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let result = self
            .counters
            .add_request(&self.counters.list_requests, self.limits);
        if let Err(error) = result {
            return futures_util::stream::once(async { Err(error) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counters
            .add_request(&self.counters.list_requests, self.limits)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn limit_error(message: String) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "kevindb_query",
        source: Box::new(std::io::Error::other(message)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::{MeasuringObjectStore, ObjectStoreReadLimits};

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

    #[tokio::test]
    async fn rejects_reads_that_exceed_request_limit() -> object_store::Result<()> {
        let store = MeasuringObjectStore::with_limits(
            Arc::new(InMemory::new()),
            ObjectStoreReadLimits {
                max_requests: Some(0),
                max_bytes_read: None,
            },
        );
        let path = Path::from("query/a.txt");
        store.put(&path, PutPayload::from_static(b"abcdef")).await?;

        let error = store
            .get(&path)
            .await
            .expect_err("request limit should reject read");
        assert!(error.to_string().contains("actual object-store requests"));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_streams_that_exceed_byte_limit() -> object_store::Result<()> {
        let store = MeasuringObjectStore::with_limits(
            Arc::new(InMemory::new()),
            ObjectStoreReadLimits {
                max_requests: Some(10),
                max_bytes_read: Some(2),
            },
        );
        let path = Path::from("query/a.txt");
        store.put(&path, PutPayload::from_static(b"abcdef")).await?;

        let error = store
            .get(&path)
            .await?
            .bytes()
            .await
            .expect_err("byte limit should reject stream");
        assert!(error.to_string().contains("actual bytes read"));
        Ok(())
    }
}
