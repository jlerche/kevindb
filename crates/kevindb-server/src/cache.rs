use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder,
};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result, UploadPart,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct CachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: CacheBackend,
    keys_by_path: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

impl CachedObjectStore {
    pub fn memory(inner: Arc<dyn ObjectStore>, capacity_bytes: usize) -> Self {
        Self::new(
            inner,
            CacheBackend::Memory(
                CacheBuilder::new(capacity_bytes)
                    .with_name("kevindb-object-store-cache")
                    .with_weighter(|_key: &String, value: &CachedObjectRead| value.weight())
                    .build(),
            ),
        )
    }

    pub async fn hybrid(
        inner: Arc<dyn ObjectStore>,
        memory_capacity_bytes: usize,
        cache_dir: impl AsRef<std::path::Path>,
        disk_capacity_bytes: usize,
        disk_block_bytes: usize,
    ) -> anyhow::Result<Self> {
        let device = FsDeviceBuilder::new(cache_dir)
            .with_capacity(disk_capacity_bytes)
            .build()?;
        let cache = HybridCacheBuilder::new()
            .with_name("kevindb-object-store-cache")
            .memory(memory_capacity_bytes)
            .with_weighter(|_key: &String, value: &CachedObjectRead| value.weight())
            .storage()
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(disk_block_bytes))
            .build()
            .await?;
        Ok(Self::new(inner, CacheBackend::Hybrid(cache)))
    }

    fn new(inner: Arc<dyn ObjectStore>, cache: CacheBackend) -> Self {
        Self {
            inner,
            cache,
            keys_by_path: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn lookup(&self, key: &str) -> Option<CachedObjectRead> {
        match &self.cache {
            CacheBackend::Memory(cache) => cache.get(key).map(|entry| entry.value().clone()),
            CacheBackend::Hybrid(cache) => match cache.get(key).await {
                Ok(entry) => entry.map(|entry| entry.value().clone()),
                Err(error) => {
                    tracing::warn!(%error, "object cache lookup failed");
                    None
                }
            },
        }
    }

    fn insert(&self, path: &str, key: String, read: CachedObjectRead) {
        match &self.cache {
            CacheBackend::Memory(cache) => {
                cache.insert(key.clone(), read);
            }
            CacheBackend::Hybrid(cache) => {
                cache.insert(key.clone(), read);
            }
        }
        self.keys_by_path
            .lock()
            .expect("cache key index mutex poisoned")
            .entry(path.to_owned())
            .or_default()
            .insert(key);
        crate::metrics::record_cache_write();
    }

    fn invalidate_path(&self, path: &Path) {
        let path = path.to_string();
        let keys = self
            .keys_by_path
            .lock()
            .expect("cache key index mutex poisoned")
            .remove(&path)
            .unwrap_or_default();
        for key in keys {
            match &self.cache {
                CacheBackend::Memory(cache) => {
                    cache.remove(&key);
                }
                CacheBackend::Hybrid(cache) => {
                    cache.remove(&key);
                }
            }
        }
        crate::metrics::record_cache_invalidation(1);
    }

    fn is_cacheable_get(options: &GetOptions) -> bool {
        !options.head
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none()
            && options.extensions.is_empty()
    }
}

impl fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedObjectStore({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let payload_bytes = payload_to_bytes(&payload);
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.invalidate_path(location);
        self.insert(
            location.as_ref(),
            cache_key(location.as_ref(), None),
            CachedObjectRead::from_put(location, payload_bytes, &result),
        );
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CachedMultipartUpload {
            inner: upload,
            cache: self.clone(),
            location: location.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !Self::is_cacheable_get(&options) {
            return self.inner.get_opts(location, options).await;
        }

        let path = location.to_string();
        let request_key = cache_key(&path, options.range.as_ref());
        if let Some(read) = self.lookup(&request_key).await {
            crate::metrics::record_cache_hit();
            return read.into_get_result(Attributes::new());
        }
        if options.range.is_some()
            && let Some(full_read) = self.lookup(&cache_key(&path, None)).await
            && let Some(read) = full_read.slice(options.range.as_ref())
        {
            self.insert(&path, request_key, read.clone());
            crate::metrics::record_cache_hit();
            return read.into_get_result(Attributes::new());
        }

        let result = self.inner.get_opts(location, options).await?;
        crate::metrics::record_cache_miss();
        let attributes = result.attributes.clone();
        let read = CachedObjectRead::from_get_result(result).await?;
        self.insert(&path, request_key, read.clone());
        read.into_get_result(attributes)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut bytes = Vec::with_capacity(ranges.len());
        for range in ranges {
            let options = GetOptions::new().with_range(Some(range.clone()));
            bytes.push(self.get_opts(location, options).await?.bytes().await?);
        }
        Ok(bytes)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let cache = self.clone();
        let locations = locations
            .map_ok(move |path| {
                cache.invalidate_path(&path);
                path
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.invalidate_path(to);
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await?;
        self.invalidate_path(from);
        self.invalidate_path(to);
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum CacheBackend {
    Memory(Cache<String, CachedObjectRead>),
    Hybrid(HybridCache<String, CachedObjectRead>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedObjectRead {
    bytes: Vec<u8>,
    location: String,
    last_modified_millis: i64,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    range_start: u64,
    range_end: u64,
}

impl CachedObjectRead {
    fn from_put(location: &Path, bytes: Bytes, result: &PutResult) -> Self {
        let size = bytes.len() as u64;
        Self {
            bytes: bytes.to_vec(),
            location: location.to_string(),
            last_modified_millis: Utc::now().timestamp_millis(),
            size,
            e_tag: result.e_tag.clone(),
            version: result.version.clone(),
            range_start: 0,
            range_end: size,
        }
    }

    async fn from_get_result(result: GetResult) -> Result<Self> {
        let meta = result.meta.clone();
        let range = result.range.clone();
        let bytes = result.bytes().await?;
        Ok(Self {
            bytes: bytes.to_vec(),
            location: meta.location.to_string(),
            last_modified_millis: meta.last_modified.timestamp_millis(),
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
            range_start: range.start,
            range_end: range.end,
        })
    }

    fn into_get_result(self, attributes: Attributes) -> Result<GetResult> {
        let range = self.range_start..self.range_end;
        let bytes = Bytes::from(self.bytes);
        let last_modified = DateTime::<Utc>::from_timestamp_millis(self.last_modified_millis)
            .ok_or_else(|| object_store::Error::Generic {
                store: "kevindb-object-store-cache",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cached object has invalid last_modified_millis: {}",
                        self.last_modified_millis
                    ),
                )),
            })?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta: ObjectMeta {
                location: Path::from(self.location.as_str()),
                last_modified,
                size: self.size,
                e_tag: self.e_tag,
                version: self.version,
            },
            range,
            attributes,
        })
    }

    fn weight(&self) -> usize {
        self.bytes.len()
    }

    fn slice(&self, requested: Option<&GetRange>) -> Option<Self> {
        if self.range_start != 0 || self.range_end != self.size {
            return None;
        }
        let range = requested_range(self.size, requested)?;
        let start = usize::try_from(range.start).ok()?;
        let end = usize::try_from(range.end).ok()?;
        let bytes = self.bytes.get(start..end)?.to_vec();
        Some(Self {
            bytes,
            location: self.location.clone(),
            last_modified_millis: self.last_modified_millis,
            size: self.size,
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
            range_start: range.start,
            range_end: range.end,
        })
    }
}

fn cache_key(path: &str, range: Option<&GetRange>) -> String {
    match range {
        None => format!("get:{path}:all"),
        Some(GetRange::Bounded(range)) => {
            format!("get:{path}:bytes={}-{}", range.start, range.end)
        }
        Some(GetRange::Offset(offset)) => format!("get:{path}:offset={offset}"),
        Some(GetRange::Suffix(suffix)) => format!("get:{path}:suffix={suffix}"),
    }
}

fn requested_range(size: u64, range: Option<&GetRange>) -> Option<Range<u64>> {
    match range {
        None => Some(0..size),
        Some(GetRange::Bounded(range)) if range.start <= range.end && range.end <= size => {
            Some(range.clone())
        }
        Some(GetRange::Offset(offset)) if *offset <= size => Some(*offset..size),
        Some(GetRange::Suffix(suffix)) => {
            let start = size.saturating_sub(*suffix);
            Some(start..size)
        }
        _ => None,
    }
}

fn payload_to_bytes(payload: &PutPayload) -> Bytes {
    if payload.as_ref().len() == 1 {
        return payload.as_ref()[0].clone();
    }
    let mut bytes = Vec::with_capacity(payload.content_length());
    for chunk in payload {
        bytes.extend_from_slice(chunk);
    }
    Bytes::from(bytes)
}

#[derive(Debug)]
struct CachedMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    cache: CachedObjectStore,
    location: Path,
}

#[async_trait]
impl MultipartUpload for CachedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.inner.complete().await?;
        self.cache.invalidate_path(&self.location);
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    #[tokio::test]
    async fn serves_repeated_reads_from_memory_cache() -> Result<()> {
        let inner = Arc::new(InMemory::new());
        let store = CachedObjectStore::memory(inner.clone(), 1024);
        let path = Path::from("segments/a.vortex");
        inner
            .put(&path, PutPayload::from_static(b"cached-object"))
            .await?;

        assert_eq!(
            store.get(&path).await?.bytes().await?,
            b"cached-object".as_slice()
        );
        inner.delete(&path).await?;

        assert_eq!(
            store.get(&path).await?.bytes().await?,
            b"cached-object".as_slice()
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalidates_cached_reads_after_wrapper_write() -> Result<()> {
        let store = CachedObjectStore::memory(Arc::new(InMemory::new()), 1024);
        let path = Path::from("segments/a.vortex");
        store.put(&path, PutPayload::from_static(b"first")).await?;
        assert_eq!(store.get(&path).await?.bytes().await?, b"first".as_slice());

        store.put(&path, PutPayload::from_static(b"second")).await?;
        assert_eq!(store.get(&path).await?.bytes().await?, b"second".as_slice());
        Ok(())
    }

    #[tokio::test]
    async fn caches_bounded_range_reads() -> Result<()> {
        let inner = Arc::new(InMemory::new());
        let store = CachedObjectStore::memory(inner.clone(), 1024);
        let path = Path::from("segments/a.vortex");
        inner
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await?;

        assert_eq!(store.get_range(&path, 2..5).await?, b"234".as_slice());
        inner.delete(&path).await?;
        assert_eq!(store.get_range(&path, 2..5).await?, b"234".as_slice());
        Ok(())
    }

    #[tokio::test]
    async fn write_through_l0_cache_serves_writer_local_range_reads() -> Result<()> {
        let inner = Arc::new(InMemory::new());
        let writer = CachedObjectStore::memory(inner.clone(), 1024);
        let path = Path::from("segments/recent.search.fst");

        writer
            .put(&path, PutPayload::from_static(b"writer-local-l0"))
            .await?;
        inner.delete(&path).await?;

        assert_eq!(writer.get_range(&path, 7..12).await?, b"local".as_slice());
        Ok(())
    }

    #[test]
    fn rejects_invalid_cached_timestamps() {
        let read = CachedObjectRead {
            bytes: Vec::new(),
            location: "segments/corrupt.vortex".to_owned(),
            last_modified_millis: i64::MAX,
            size: 0,
            e_tag: None,
            version: None,
            range_start: 0,
            range_end: 0,
        };

        assert!(read.into_get_result(Attributes::new()).is_err());
    }

    #[tokio::test]
    async fn empty_node_cache_falls_back_to_object_store_l1() -> Result<()> {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("segments/durable.search.fst");
        {
            let writer = CachedObjectStore::memory(inner.clone(), 1024);
            writer
                .put(&path, PutPayload::from_static(b"durable-l1-object"))
                .await?;
        }

        let reader = CachedObjectStore::memory(inner, 1024);
        assert_eq!(reader.get_range(&path, 8..10).await?, b"l1".as_slice());
        Ok(())
    }
}
