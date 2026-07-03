use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:3000";
pub const DEFAULT_INGEST_MAX_FLUSH_DELAY_MS: u64 = 500;
pub const DEFAULT_INGEST_MAX_SPANS_PER_SEGMENT: usize = 1024;
pub const DEFAULT_OBJECT_STORE: &str = "memory";
pub const ENV_BIND_ADDR: &str = "KEVINDB_BIND_ADDR";
pub const ENV_INGEST_MAX_FLUSH_DELAY_MS: &str = "KEVINDB_INGEST_MAX_FLUSH_DELAY_MS";
pub const ENV_INGEST_MAX_SPANS_PER_SEGMENT: &str = "KEVINDB_INGEST_MAX_SPANS_PER_SEGMENT";
pub const ENV_OBJECT_STORE: &str = "KEVINDB_OBJECT_STORE";
pub const ENV_POSTGRES_URL: &str = "KEVINDB_POSTGRES_URL";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub postgres_url: String,
    pub bind_addr: SocketAddr,
    pub object_store: ObjectStoreConfig,
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
        let object_store = ObjectStoreConfig::parse(
            &lookup(ENV_OBJECT_STORE).unwrap_or_else(|| DEFAULT_OBJECT_STORE.to_owned()),
        )?;
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
            object_store,
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
pub enum ConfigError {
    MissingEnv { name: &'static str },
    InvalidBindAddr { value: String },
    InvalidPositiveInteger { name: &'static str, value: String },
    InvalidUnsignedInteger { name: &'static str, value: String },
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
        assert_eq!(
            config.bind_addr,
            DEFAULT_BIND_ADDR.parse::<SocketAddr>().expect("bind addr")
        );
        assert_eq!(config.object_store, ObjectStoreConfig::Memory);
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
            (ENV_OBJECT_STORE, "MEMORY"),
            (ENV_INGEST_MAX_SPANS_PER_SEGMENT, "2048"),
            (ENV_INGEST_MAX_FLUSH_DELAY_MS, "25"),
        ])
        .expect("parse config");

        assert_eq!(config.bind_addr, "0.0.0.0:8080".parse().expect("bind addr"));
        assert_eq!(config.object_store, ObjectStoreConfig::Memory);
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
}
