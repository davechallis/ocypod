//! Configuration parsing.

use std::path::Path;
use std::fs;
use std::default::Default;
use std::str::FromStr;

use toml;
use serde::de::{Deserialize, Deserializer, Error};
use serde_derive::*;
use human_size;
use log::debug;

use crate::models::Duration;

// TODO: additional config
// * keepalive
// https://actix.rs/docs/server/

/// Main application config, typically read from a `.toml` file.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    /// Configuration for the application's HTTP server.
    pub server: ServerConfig,

    /// Configuration for connecting to Redis.
    pub redis: RedisConfig,
}

impl Config {
    /// Read configuration from a file into a new Config struct.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();
        debug!("Reading configuration from {}", path.display());

        let data = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(err) => return Err(err.to_string()),
        };

        let conf: Config = match toml::from_str(&data) {
            Ok(conf) => conf,
            Err(err) => return Err(err.to_string()),
        };

        Ok(conf)
    }

    /// Get the address for the HTTP server to listen on.
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    pub fn redis_url(&self) -> &str {
        &self.redis.url
    }
}

/// Configuration for the application's HTTP server.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Host address to listen on. Defaults to "127.0.0.1" if not specified.
    pub host: String,

    /// Port to listen on. Defaults to 8023 if not specified.
    pub port: u16,

    /// Number of HTTP worker threads. Defaults to number of CPUs if not specified.
    pub threads: Option<usize>,

    /// Maximum size in bytes for HTTP POST requests. Defaults to "256kB" if not specified.
    #[serde(deserialize_with = "deserialize_human_size")]
    pub max_body_size: Option<usize>,

    /// Determines how often running tasks are checked for timeouts. Defaults to "30s" if not specified.
    pub timeout_check_interval: Duration,

    /// Determines how often failed tasks are checked for retrying. Defaults to "60s" if not specified.
    pub retry_check_interval: Duration,

    /// Determines how often ended tasks are checked for expiry. Defaults to "5m" if not specified.
    pub expiry_check_interval: Duration,

    /// Amount of time workers have to finish requests after server receives SIGTERM.
    pub shutdown_timeout: Option<Duration>,

    pub next_job_delay: Option<Duration>,

    #[serde(deserialize_with = "deserialize_log_level")]
    pub log_level: log::Level,
}

fn deserialize_human_size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<usize>, D::Error> {
    let s: Option<&str> = Deserialize::deserialize(deserializer)?;
    Ok(match s {
        Some(s) => {
            let size: human_size::SpecificSize<human_size::Byte> = match s.parse() {
                Ok(size) => size,
                Err(_) => return Err(Error::custom(format!("Unable to parse size '{}'", s))),
            };
            Some(size.value() as usize)
        },
        None => None,
    })
}

fn deserialize_log_level<'de, D: Deserializer<'de>>(deserializer: D) -> Result<log::Level, D::Error> {
    let s: &str = Deserialize::deserialize(deserializer)?;
    match log::Level::from_str(s) {
        Ok(level) => Ok(level),
        Err(_) => Err(Error::custom(format!("Invalid log level: {}", s))),
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 8023,
            threads: None,
            max_body_size: None,
            timeout_check_interval: Duration::from_secs(30),
            retry_check_interval: Duration::from_secs(60),
            expiry_check_interval: Duration::from_secs(300),
            shutdown_timeout: None,
            next_job_delay: None,
            log_level: log::Level::Info,
        }
    }
}

/// Configuration for connecting to Redis.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    /// Redis URL to connect to. Defaults to "redis://127.0.0.1".
    pub url: String,

    // TODO: determine sensible default
    /// Number of connections to Redis that will be spawned.
    pub threads: Option<usize>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            url: "redis://127.0.0.1".to_owned(),
            threads: None,
        }
    }
}
