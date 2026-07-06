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
pub const DEFAULT_OBJECT_STORE: &str = "memory";
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub postgres_url: String,
    pub bind_addr: SocketAddr,
    pub node_id: Option<String>,
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
        let node_id = lookup(ENV_NODE_ID).and_then(|value| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_owned())
        });
        let object_store = ObjectStoreConfig::parse(
            &lookup(ENV_OBJECT_STORE).unwrap_or_else(|| DEFAULT_OBJECT_STORE.to_owned()),
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
            node_id,
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
pub enum ObjectStoreConfig {
    Memory,
}

impl ObjectStoreConfig {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.to_ascii_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            _ => Err(ConfigError::UnsupportedObjectStore {
                value: value.to_owned(),
            }),
        }
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
    InvalidPositiveInteger { name: &'static str, value: String },
    InvalidUnsignedInteger { name: &'static str, value: String },
    UnsupportedCacheMode { value: String },
    UnsupportedObjectStore { value: String },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnv { name } => write!(f, "{name} must be set"),
            Self::InvalidBindAddr { value } => {
                write!(f, "{ENV_BIND_ADDR} must be a socket address, got {value}")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_postgres_url_with_defaults() {
        let config = ServerConfig::from_env_vars([(ENV_POSTGRES_URL, "postgresql://db/postgres")])
            .expect("parse config");

        assert_eq!(config.postgres_url, "postgresql://db/postgres");
        assert_eq!(config.node_id, None);
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
        let config = ServerConfig::from_env_vars([
            (ENV_POSTGRES_URL, "postgresql://db/postgres"),
            (ENV_BIND_ADDR, "0.0.0.0:8080"),
            (ENV_NODE_ID, "node-a"),
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
    fn rejects_missing_postgres_url() {
        let error = ServerConfig::from_env_vars(Vec::<(String, String)>::new())
            .expect_err("missing postgres url");

        assert_eq!(
            error,
            ConfigError::MissingEnv {
                name: ENV_POSTGRES_URL
            }
        );
    }

    #[test]
    fn rejects_invalid_bind_addr() {
        let error = ServerConfig::from_env_vars([
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
        let error = ServerConfig::from_env_vars([
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
    fn rejects_unsupported_cache_mode() {
        let error = ServerConfig::from_env_vars([
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
        let error = ServerConfig::from_env_vars([
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
        let error = ServerConfig::from_env_vars([
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
        let error = ServerConfig::from_env_vars([
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
        let error = ServerConfig::from_env_vars([
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
