use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:3000";
pub const DEFAULT_CACHE_DISK_CAPACITY_BYTES: usize = 1024 * 1024 * 1024;
pub const DEFAULT_CACHE_DISK_BLOCK_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_CACHE_MEMORY_CAPACITY_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_CACHE_MODE: &str = "memory";
pub const DEFAULT_INGEST_MAX_FLUSH_DELAY_MS: u64 = 500;
pub const DEFAULT_INGEST_MAX_SPANS_PER_SEGMENT: usize = 1024;
pub const DEFAULT_RUN_MIGRATIONS: bool = true;
pub const DEFAULT_SERVICE_ROLE: &str = "all";
pub const ENV_BIND_ADDR: &str = "KEVINDB_BIND_ADDR";
pub const ENV_CACHE_DISK_BLOCK_BYTES: &str = "KEVINDB_CACHE_DISK_BLOCK_BYTES";
pub const ENV_CACHE_DISK_CAPACITY_BYTES: &str = "KEVINDB_CACHE_DISK_CAPACITY_BYTES";
pub const ENV_CACHE_HYBRID_DIR: &str = "KEVINDB_CACHE_HYBRID_DIR";
pub const ENV_CACHE_MEMORY_CAPACITY_BYTES: &str = "KEVINDB_CACHE_MEMORY_CAPACITY_BYTES";
pub const ENV_CACHE_MODE: &str = "KEVINDB_CACHE_MODE";
pub const ENV_INGEST_MAX_FLUSH_DELAY_MS: &str = "KEVINDB_INGEST_MAX_FLUSH_DELAY_MS";
pub const ENV_INGEST_MAX_SPANS_PER_SEGMENT: &str = "KEVINDB_INGEST_MAX_SPANS_PER_SEGMENT";
pub const ENV_NODE_ID: &str = "KEVINDB_NODE_ID";
pub const ENV_OBJECT_STORE: &str = "KEVINDB_OBJECT_STORE";
pub const ENV_POSTGRES_URL: &str = "KEVINDB_POSTGRES_URL";
pub const ENV_RUN_MIGRATIONS: &str = "KEVINDB_RUN_MIGRATIONS";
pub const ENV_S3_ALLOW_HTTP: &str = "KEVINDB_S3_ALLOW_HTTP";
pub const ENV_S3_BUCKET: &str = "KEVINDB_S3_BUCKET";
pub const ENV_S3_ENDPOINT: &str = "KEVINDB_S3_ENDPOINT";
pub const ENV_S3_PREFIX: &str = "KEVINDB_S3_PREFIX";
pub const ENV_S3_REGION: &str = "KEVINDB_S3_REGION";
pub const ENV_SERVICE_ROLE: &str = "KEVINDB_SERVICE_ROLE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub postgres_url: String,
    pub bind_addr: SocketAddr,
    pub run_migrations: bool,
    pub node_id: Option<String>,
    pub service_role: ServiceRole,
    pub object_store: ObjectStoreConfig,
    pub cache: CacheConfig,
    pub ingest: IngestConfig,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_env_vars<I, K, V>(vars: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let vars = vars
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<HashMap<_, _>>();
        Self::from_lookup(|name| vars.get(name).cloned())
    }

    pub fn from_lookup<F>(mut lookup: F) -> Result<Self, ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let postgres_url = lookup(ENV_POSTGRES_URL).ok_or(ConfigError::MissingEnv {
            name: ENV_POSTGRES_URL,
        })?;
        let bind_addr =
            parse_bind_addr(lookup(ENV_BIND_ADDR).unwrap_or_else(|| DEFAULT_BIND_ADDR.to_owned()))?;
        let run_migrations = parse_bool(
            ENV_RUN_MIGRATIONS,
            lookup(ENV_RUN_MIGRATIONS).unwrap_or_else(|| DEFAULT_RUN_MIGRATIONS.to_string()),
        )?;
        let node_id = lookup(ENV_NODE_ID).and_then(|value| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_owned())
        });
        let object_store = ObjectStoreConfig::from_lookup(&mut lookup)?;
        let service_role = ServiceRole::parse(
            &lookup(ENV_SERVICE_ROLE).unwrap_or_else(|| DEFAULT_SERVICE_ROLE.to_owned()),
        )?;
        let cache = CacheConfig::from_lookup(&mut lookup)?;
        let ingest = IngestConfig {
            max_spans_per_segment: parse_positive_usize(
                ENV_INGEST_MAX_SPANS_PER_SEGMENT,
                lookup(ENV_INGEST_MAX_SPANS_PER_SEGMENT)
                    .unwrap_or_else(|| DEFAULT_INGEST_MAX_SPANS_PER_SEGMENT.to_string()),
            )?,
            max_flush_delay: Duration::from_millis(parse_u64(
                ENV_INGEST_MAX_FLUSH_DELAY_MS,
                lookup(ENV_INGEST_MAX_FLUSH_DELAY_MS)
                    .unwrap_or_else(|| DEFAULT_INGEST_MAX_FLUSH_DELAY_MS.to_string()),
            )?),
        };

        Ok(Self {
            postgres_url,
            bind_addr,
            run_migrations,
            node_id,
            service_role,
            object_store,
            cache,
            ingest,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestConfig {
    pub max_spans_per_segment: usize,
    pub max_flush_delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceRole {
    All,
    Ingest,
    Query,
    Compaction,
    Coordinator,
}

impl ServiceRole {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "ingest" => Ok(Self::Ingest),
            "query" => Ok(Self::Query),
            "compaction" => Ok(Self::Compaction),
            "coordinator" => Ok(Self::Coordinator),
            _ => Err(ConfigError::UnsupportedServiceRole {
                value: value.to_owned(),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Ingest => "ingest",
            Self::Query => "query",
            Self::Compaction => "compaction",
            Self::Coordinator => "coordinator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreKind {
    Memory,
    S3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectStoreConfig {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    Memory,
    S3(S3ObjectStoreConfig),
}

impl ObjectStoreConfig {
    fn from_lookup<F>(lookup: &mut F) -> Result<Self, ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        match object_store_kind(&non_empty_env(ENV_OBJECT_STORE, lookup(ENV_OBJECT_STORE))?)? {
            ObjectStoreKind::Memory => Ok(Self::Memory),
            ObjectStoreKind::S3 => Ok(Self::S3(S3ObjectStoreConfig {
                bucket: non_empty_env(ENV_S3_BUCKET, lookup(ENV_S3_BUCKET))?,
                region: optional_non_empty(lookup(ENV_S3_REGION)),
                endpoint: optional_non_empty(lookup(ENV_S3_ENDPOINT)),
                allow_http: parse_bool(
                    ENV_S3_ALLOW_HTTP,
                    lookup(ENV_S3_ALLOW_HTTP).unwrap_or_else(|| "false".to_owned()),
                )?,
                prefix: non_empty_env(ENV_S3_PREFIX, lookup(ENV_S3_PREFIX))?,
            })),
        }
    }
}

fn object_store_kind(value: &str) -> Result<ObjectStoreKind, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "memory" => Ok(ObjectStoreKind::Memory),
        "s3" => Ok(ObjectStoreKind::S3),
        _ => Err(ConfigError::UnsupportedObjectStore {
            value: value.to_owned(),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheConfig {
    pub mode: CacheMode,
    pub memory_capacity_bytes: usize,
    pub hybrid_dir: Option<String>,
    pub disk_capacity_bytes: usize,
    pub disk_block_bytes: usize,
}

impl CacheConfig {
    fn from_lookup<F>(lookup: &mut F) -> Result<Self, ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mode = CacheMode::parse(
            &lookup(ENV_CACHE_MODE).unwrap_or_else(|| DEFAULT_CACHE_MODE.to_owned()),
        )?;
        let memory_capacity_bytes = parse_positive_usize(
            ENV_CACHE_MEMORY_CAPACITY_BYTES,
            lookup(ENV_CACHE_MEMORY_CAPACITY_BYTES)
                .unwrap_or_else(|| DEFAULT_CACHE_MEMORY_CAPACITY_BYTES.to_string()),
        )?;
        let hybrid_dir = lookup(ENV_CACHE_HYBRID_DIR);
        if mode == CacheMode::Hybrid && hybrid_dir.as_deref().unwrap_or_default().is_empty() {
            return Err(ConfigError::MissingEnv {
                name: ENV_CACHE_HYBRID_DIR,
            });
        }
        let disk_capacity_bytes = parse_positive_usize(
            ENV_CACHE_DISK_CAPACITY_BYTES,
            lookup(ENV_CACHE_DISK_CAPACITY_BYTES)
                .unwrap_or_else(|| DEFAULT_CACHE_DISK_CAPACITY_BYTES.to_string()),
        )?;
        let disk_block_bytes = parse_positive_usize(
            ENV_CACHE_DISK_BLOCK_BYTES,
            lookup(ENV_CACHE_DISK_BLOCK_BYTES)
                .unwrap_or_else(|| DEFAULT_CACHE_DISK_BLOCK_BYTES.to_string()),
        )?;

        Ok(Self {
            mode,
            memory_capacity_bytes,
            hybrid_dir,
            disk_capacity_bytes,
            disk_block_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    Memory,
    Hybrid,
}

impl CacheMode {
    fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.to_ascii_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "hybrid" => Ok(Self::Hybrid),
            _ => Err(ConfigError::UnsupportedCacheMode {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    MissingEnv { name: &'static str },
    InvalidBindAddr { value: String },
    InvalidBool { name: &'static str, value: String },
    InvalidPositiveInteger { name: &'static str, value: String },
    InvalidUnsignedInteger { name: &'static str, value: String },
    UnsupportedCacheMode { value: String },
    UnsupportedObjectStore { value: String },
    UnsupportedServiceRole { value: String },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnv { name } => write!(f, "{name} must be set"),
            Self::InvalidBindAddr { value } => {
                write!(f, "{ENV_BIND_ADDR} must be a socket address, got {value}")
            }
            Self::InvalidBool { name, value } => {
                write!(f, "{name} must be true or false, got {value}")
            }
            Self::InvalidPositiveInteger { name, value } => {
                write!(f, "{name} must be a positive integer, got {value}")
            }
            Self::InvalidUnsignedInteger { name, value } => {
                write!(f, "{name} must be an unsigned integer, got {value}")
            }
            Self::UnsupportedCacheMode { value } => {
                write!(f, "{ENV_CACHE_MODE}={value} is not supported")
            }
            Self::UnsupportedObjectStore { value } => {
                write!(f, "{ENV_OBJECT_STORE}={value} is not supported")
            }
            Self::UnsupportedServiceRole { value } => {
                write!(f, "{ENV_SERVICE_ROLE}={value} is not supported")
            }
        }
    }
}

impl Error for ConfigError {}

fn parse_bind_addr(value: String) -> Result<SocketAddr, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::InvalidBindAddr { value })
}

fn parse_positive_usize(name: &'static str, value: String) -> Result<usize, ConfigError> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err(ConfigError::InvalidPositiveInteger { name, value }),
    }
}

fn parse_u64(name: &'static str, value: String) -> Result<u64, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::InvalidUnsignedInteger { name, value })
}

fn parse_bool(name: &'static str, value: String) -> Result<bool, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::InvalidBool { name, value }),
    }
}

fn non_empty_env(name: &'static str, value: Option<String>) -> Result<String, ConfigError> {
    optional_non_empty(value).ok_or(ConfigError::MissingEnv { name })
}

fn optional_non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().trim_matches('/').to_owned();
        (!value.is_empty()).then_some(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config<I, K, V>(vars: I) -> Result<ServerConfig, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut vars = vars
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<Vec<_>>();
        if !vars.iter().any(|(key, _)| key == ENV_OBJECT_STORE) {
            vars.push((ENV_OBJECT_STORE.to_owned(), "memory".to_owned()));
        }
        ServerConfig::from_env_vars(vars)
    }

    #[test]
    fn parses_required_postgres_url_with_defaults() {
        let config =
            test_config([(ENV_POSTGRES_URL, "postgresql://db/postgres")]).expect("parse config");

        assert_eq!(config.postgres_url, "postgresql://db/postgres");
        assert_eq!(config.node_id, None);
        assert!(config.run_migrations);
        assert_eq!(config.service_role, ServiceRole::All);
        assert_eq!(
            config.bind_addr,
            DEFAULT_BIND_ADDR.parse::<SocketAddr>().expect("bind addr")
        );
        assert_eq!(config.object_store, ObjectStoreConfig::Memory);
        assert_eq!(
            config.cache,
            CacheConfig {
                mode: CacheMode::Memory,
                memory_capacity_bytes: DEFAULT_CACHE_MEMORY_CAPACITY_BYTES,
                hybrid_dir: None,
                disk_capacity_bytes: DEFAULT_CACHE_DISK_CAPACITY_BYTES,
                disk_block_bytes: DEFAULT_CACHE_DISK_BLOCK_BYTES,
            }
        );
        assert_eq!(
            config.ingest,
            IngestConfig {
                max_spans_per_segment: DEFAULT_INGEST_MAX_SPANS_PER_SEGMENT,
                max_flush_delay: Duration::from_millis(DEFAULT_INGEST_MAX_FLUSH_DELAY_MS),
            }
        );
    }

    #[test]
    fn parses_explicit_server_values() {
        let config = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_BIND_ADDR, "0.0.0.0:8080"),
            (ENV_NODE_ID, "node-a"),
            (ENV_RUN_MIGRATIONS, "false"),
            (ENV_SERVICE_ROLE, "query"),
            (ENV_OBJECT_STORE, "MEMORY"),
            (ENV_CACHE_MODE, "hybrid"),
            (ENV_CACHE_HYBRID_DIR, "/tmp/kevindb-cache"),
            (ENV_CACHE_MEMORY_CAPACITY_BYTES, "1048576"),
            (ENV_CACHE_DISK_CAPACITY_BYTES, "33554432"),
            (ENV_CACHE_DISK_BLOCK_BYTES, "4194304"),
            (ENV_INGEST_MAX_SPANS_PER_SEGMENT, "2048"),
            (ENV_INGEST_MAX_FLUSH_DELAY_MS, "25"),
        ])
        .expect("parse config");

        assert_eq!(config.bind_addr, "0.0.0.0:8080".parse().expect("bind addr"));
        assert_eq!(config.node_id.as_deref(), Some("node-a"));
        assert!(!config.run_migrations);
        assert_eq!(config.service_role, ServiceRole::Query);
        assert_eq!(config.object_store, ObjectStoreConfig::Memory);
        assert_eq!(
            config.cache,
            CacheConfig {
                mode: CacheMode::Hybrid,
                memory_capacity_bytes: 1_048_576,
                hybrid_dir: Some("/tmp/kevindb-cache".to_owned()),
                disk_capacity_bytes: 33_554_432,
                disk_block_bytes: 4_194_304,
            }
        );
        assert_eq!(
            config.ingest,
            IngestConfig {
                max_spans_per_segment: 2048,
                max_flush_delay: Duration::from_millis(25),
            }
        );
    }

    #[test]
    fn parses_s3_object_store_values() {
        let config = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_OBJECT_STORE, "s3"),
            (ENV_S3_BUCKET, "kevindb"),
            (ENV_S3_REGION, "us-east-1"),
            (ENV_S3_ENDPOINT, "http://minio:9000"),
            (ENV_S3_ALLOW_HTTP, "true"),
            (ENV_S3_PREFIX, "/dev/"),
        ])
        .expect("parse s3 config");

        assert_eq!(
            config.object_store,
            ObjectStoreConfig::S3(S3ObjectStoreConfig {
                bucket: "kevindb".to_owned(),
                region: Some("us-east-1".to_owned()),
                endpoint: Some("http://minio:9000".to_owned()),
                allow_http: true,
                prefix: "dev".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_postgres_url() {
        let error = test_config(Vec::<(String, String)>::new()).expect_err("missing postgres url");

        assert_eq!(
            error,
            ConfigError::MissingEnv {
                name: ENV_POSTGRES_URL
            }
        );
    }

    #[test]
    fn rejects_missing_object_store() {
        let error = ServerConfig::from_env_vars([(ENV_POSTGRES_URL, "postgresql://db/postgres")])
            .expect_err("missing object store");

        assert_eq!(
            error,
            ConfigError::MissingEnv {
                name: ENV_OBJECT_STORE
            }
        );
    }

    #[test]
    fn rejects_invalid_bind_addr() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_BIND_ADDR, "not-a-socket"),
        ])
        .expect_err("invalid bind addr");

        assert_eq!(
            error,
            ConfigError::InvalidBindAddr {
                value: "not-a-socket".to_owned()
            }
        );
    }

    #[test]
    fn rejects_unsupported_object_store() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_OBJECT_STORE, "local"),
        ])
        .expect_err("unsupported object store");

        assert_eq!(
            error,
            ConfigError::UnsupportedObjectStore {
                value: "local".to_owned()
            }
        );
    }

    #[test]
    fn rejects_s3_without_bucket() {
        let err = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_OBJECT_STORE, "s3"),
        ])
        .expect_err("s3 bucket is required");

        assert_eq!(
            err,
            ConfigError::MissingEnv {
                name: ENV_S3_BUCKET
            }
        );
    }

    #[test]
    fn rejects_s3_without_prefix() {
        let err = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_OBJECT_STORE, "s3"),
            (ENV_S3_BUCKET, "kevindb"),
        ])
        .expect_err("s3 prefix is required");

        assert_eq!(
            err,
            ConfigError::MissingEnv {
                name: ENV_S3_PREFIX
            }
        );
    }

    #[test]
    fn rejects_invalid_s3_allow_http() {
        let err = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_OBJECT_STORE, "s3"),
            (ENV_S3_BUCKET, "kevindb"),
            (ENV_S3_ALLOW_HTTP, "sometimes"),
        ])
        .expect_err("s3 allow_http must be boolean");

        assert_eq!(
            err,
            ConfigError::InvalidBool {
                name: ENV_S3_ALLOW_HTTP,
                value: "sometimes".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_invalid_run_migrations_bool() {
        let err = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_RUN_MIGRATIONS, "sometimes"),
        ])
        .expect_err("run migrations flag must be boolean");

        assert_eq!(
            err,
            ConfigError::InvalidBool {
                name: ENV_RUN_MIGRATIONS,
                value: "sometimes".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_unsupported_service_role() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_SERVICE_ROLE, "everything"),
        ])
        .expect_err("unsupported service role");

        assert_eq!(
            error,
            ConfigError::UnsupportedServiceRole {
                value: "everything".to_owned()
            }
        );
    }

    #[test]
    fn rejects_unsupported_cache_mode() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_CACHE_MODE, "disk"),
        ])
        .expect_err("unsupported cache mode");

        assert_eq!(
            error,
            ConfigError::UnsupportedCacheMode {
                value: "disk".to_owned()
            }
        );
    }

    #[test]
    fn rejects_hybrid_cache_without_dir() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_CACHE_MODE, "hybrid"),
        ])
        .expect_err("missing hybrid cache dir");

        assert_eq!(
            error,
            ConfigError::MissingEnv {
                name: ENV_CACHE_HYBRID_DIR
            }
        );
    }

    #[test]
    fn rejects_zero_ingest_segment_size() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_INGEST_MAX_SPANS_PER_SEGMENT, "0"),
        ])
        .expect_err("zero segment size");

        assert_eq!(
            error,
            ConfigError::InvalidPositiveInteger {
                name: ENV_INGEST_MAX_SPANS_PER_SEGMENT,
                value: "0".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_invalid_ingest_flush_delay() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_INGEST_MAX_FLUSH_DELAY_MS, "soon"),
        ])
        .expect_err("invalid flush delay");

        assert_eq!(
            error,
            ConfigError::InvalidUnsignedInteger {
                name: ENV_INGEST_MAX_FLUSH_DELAY_MS,
                value: "soon".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_invalid_cache_size() {
        let error = test_config([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_CACHE_MEMORY_CAPACITY_BYTES, "0"),
        ])
        .expect_err("zero cache capacity");

        assert_eq!(
            error,
            ConfigError::InvalidPositiveInteger {
                name: ENV_CACHE_MEMORY_CAPACITY_BYTES,
                value: "0".to_owned(),
            }
        );
    }
}
