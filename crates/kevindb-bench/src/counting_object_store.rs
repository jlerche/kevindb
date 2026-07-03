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
    UploadPart,
};
use serde::Serialize;

#[derive(Debug, Default)]
pub struct ObjectStoreCounters {
    put_requests: AtomicU64,
    multipart_upload_requests: AtomicU64,
    multipart_part_requests: AtomicU64,
    multipart_complete_requests: AtomicU64,
    multipart_abort_requests: AtomicU64,
    get_requests: AtomicU64,
    get_ranges_requests: AtomicU64,
    head_requests: AtomicU64,
    list_requests: AtomicU64,
    delete_requests: AtomicU64,
    copy_requests: AtomicU64,
    rename_requests: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    listed_objects: AtomicU64,
    listed_prefixes: AtomicU64,
    deleted_objects: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ObjectStoreSnapshot {
    pub put_requests: u64,
    pub multipart_upload_requests: u64,
    pub multipart_part_requests: u64,
    pub multipart_complete_requests: u64,
    pub multipart_abort_requests: u64,
    pub get_requests: u64,
    pub get_ranges_requests: u64,
    pub head_requests: u64,
    pub list_requests: u64,
    pub delete_requests: u64,
    pub copy_requests: u64,
    pub rename_requests: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub listed_objects: u64,
    pub listed_prefixes: u64,
    pub deleted_objects: u64,
}

impl ObjectStoreSnapshot {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            put_requests: self.put_requests.saturating_sub(earlier.put_requests),
            multipart_upload_requests: self
                .multipart_upload_requests
                .saturating_sub(earlier.multipart_upload_requests),
            multipart_part_requests: self
                .multipart_part_requests
                .saturating_sub(earlier.multipart_part_requests),
            multipart_complete_requests: self
                .multipart_complete_requests
                .saturating_sub(earlier.multipart_complete_requests),
            multipart_abort_requests: self
                .multipart_abort_requests
                .saturating_sub(earlier.multipart_abort_requests),
            get_requests: self.get_requests.saturating_sub(earlier.get_requests),
            get_ranges_requests: self
                .get_ranges_requests
                .saturating_sub(earlier.get_ranges_requests),
            head_requests: self.head_requests.saturating_sub(earlier.head_requests),
            list_requests: self.list_requests.saturating_sub(earlier.list_requests),
            delete_requests: self.delete_requests.saturating_sub(earlier.delete_requests),
            copy_requests: self.copy_requests.saturating_sub(earlier.copy_requests),
            rename_requests: self.rename_requests.saturating_sub(earlier.rename_requests),
            bytes_written: self.bytes_written.saturating_sub(earlier.bytes_written),
            bytes_read: self.bytes_read.saturating_sub(earlier.bytes_read),
            listed_objects: self.listed_objects.saturating_sub(earlier.listed_objects),
            listed_prefixes: self.listed_prefixes.saturating_sub(earlier.listed_prefixes),
            deleted_objects: self.deleted_objects.saturating_sub(earlier.deleted_objects),
        }
    }

    pub fn request_count(&self) -> u64 {
        self.put_requests
            + self.multipart_upload_requests
            + self.multipart_part_requests
            + self.multipart_complete_requests
            + self.multipart_abort_requests
            + self.get_requests
            + self.get_ranges_requests
            + self.head_requests
            + self.list_requests
            + self.delete_requests
            + self.copy_requests
            + self.rename_requests
    }
}

impl ObjectStoreCounters {
    pub fn snapshot(&self) -> ObjectStoreSnapshot {
        ObjectStoreSnapshot {
            put_requests: self.load(&self.put_requests),
            multipart_upload_requests: self.load(&self.multipart_upload_requests),
            multipart_part_requests: self.load(&self.multipart_part_requests),
            multipart_complete_requests: self.load(&self.multipart_complete_requests),
            multipart_abort_requests: self.load(&self.multipart_abort_requests),
            get_requests: self.load(&self.get_requests),
            get_ranges_requests: self.load(&self.get_ranges_requests),
            head_requests: self.load(&self.head_requests),
            list_requests: self.load(&self.list_requests),
            delete_requests: self.load(&self.delete_requests),
            copy_requests: self.load(&self.copy_requests),
            rename_requests: self.load(&self.rename_requests),
            bytes_written: self.load(&self.bytes_written),
            bytes_read: self.load(&self.bytes_read),
            listed_objects: self.load(&self.listed_objects),
            listed_prefixes: self.load(&self.listed_prefixes),
            deleted_objects: self.load(&self.deleted_objects),
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
pub struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<ObjectStoreCounters>,
}

impl CountingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: Arc::new(ObjectStoreCounters::default()),
        }
    }

    pub fn counters(&self) -> Arc<ObjectStoreCounters> {
        Arc::clone(&self.counters)
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.counters.add(&self.counters.put_requests, 1);
        self.counters.add(
            &self.counters.bytes_written,
            payload.content_length() as u64,
        );
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.counters
            .add(&self.counters.multipart_upload_requests, 1);
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingMultipartUpload {
            inner: upload,
            counters: Arc::clone(&self.counters),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.counters.add(&self.counters.head_requests, 1);
            return self.inner.get_opts(location, options).await;
        }

        self.counters.add(&self.counters.get_requests, 1);
        let result = self.inner.get_opts(location, options).await?;
        if result.range.is_empty() {
            return Ok(result);
        }

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
        self.counters.add(&self.counters.delete_requests, 1);
        let counters = Arc::clone(&self.counters);
        let locations = locations
            .map_ok(move |path| {
                counters.add(&counters.deleted_objects, 1);
                path
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counters.add(&self.counters.list_requests, 1);
        let counters = Arc::clone(&self.counters);
        self.inner
            .list(prefix)
            .map_ok(move |meta| {
                counters.add(&counters.listed_objects, 1);
                meta
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counters.add(&self.counters.list_requests, 1);
        let counters = Arc::clone(&self.counters);
        self.inner
            .list_with_offset(prefix, offset)
            .map_ok(move |meta| {
                counters.add(&counters.listed_objects, 1);
                meta
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counters.add(&self.counters.list_requests, 1);
        let result = self.inner.list_with_delimiter(prefix).await?;
        self.counters
            .add(&self.counters.listed_objects, result.objects.len() as u64);
        self.counters.add(
            &self.counters.listed_prefixes,
            result.common_prefixes.len() as u64,
        );
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.counters.add(&self.counters.copy_requests, 1);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.counters.add(&self.counters.rename_requests, 1);
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct CountingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<ObjectStoreCounters>,
}

#[async_trait]
impl MultipartUpload for CountingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counters.add(&self.counters.multipart_part_requests, 1);
        self.counters
            .add(&self.counters.bytes_written, data.content_length() as u64);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.counters
            .add(&self.counters.multipart_complete_requests, 1);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.counters
            .add(&self.counters.multipart_abort_requests, 1);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use super::CountingObjectStore;

    #[tokio::test]
    async fn counts_object_store_operations_and_consumed_bytes() -> object_store::Result<()> {
        let store = CountingObjectStore::new(Arc::new(InMemory::new()));
        let path = Path::from("bench/a.txt");

        store.put(&path, PutPayload::from_static(b"abcdef")).await?;
        store.head(&path).await?;
        assert_eq!(store.get_range(&path, 1..4).await?, b"bcd".as_slice());
        assert_eq!(store.get(&path).await?.bytes().await?, b"abcdef".as_slice());
        let ranges = store.get_ranges(&path, &[0..2, 4..6]).await?;
        assert_eq!(
            ranges,
            vec![Bytes::from_static(b"ab"), Bytes::from_static(b"ef")]
        );

        let listed = store
            .list(Some(&Path::from("bench")))
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(listed.len(), 1);
        store
            .list_with_delimiter(Some(&Path::from("bench")))
            .await?;
        store.delete(&path).await?;

        let snapshot = store.counters().snapshot();
        assert_eq!(snapshot.put_requests, 1);
        assert_eq!(snapshot.head_requests, 1);
        assert_eq!(snapshot.get_requests, 2);
        assert_eq!(snapshot.get_ranges_requests, 1);
        assert_eq!(snapshot.list_requests, 2);
        assert_eq!(snapshot.delete_requests, 1);
        assert_eq!(snapshot.bytes_written, 6);
        assert_eq!(snapshot.bytes_read, 13);
        assert_eq!(snapshot.listed_objects, 2);
        assert_eq!(snapshot.deleted_objects, 1);
        Ok(())
    }
}
