//! Configuration parsing.

use std::default::Default;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::collections::HashMap;

use log::{debug, warn};
use regex::Captures;
use regex::Regex;
use serde::de::Deserializer;
use serde::Deserialize;
use structopt::StructOpt;

use crate::models::Duration;

const INTERPOLATE_RE: &str = r"(?m)\$\{([A-Z][A-Z0-9_]*)(?:=([^}]+))?\}";

/// Parsed command line options when the server application is started.
#[derive(Debug, StructOpt)]
#[structopt(name = "ocypod")]
pub struct CliOpts {
    #[structopt(parse(from_os_str), help = "Path to configuration file")]
    config: Option<PathBuf>,
}

/// Parses configuration from either configuration path specified in command line arguments,
/// or using default configuration if no configuration file was specified.
pub fn parse_config_from_cli_args() -> Config {
    let opts = CliOpts::from_args();
    let conf = match opts.config {
        Some(config_path) => match Config::from_file(&config_path) {
            Ok(config) => config,
            Err(msg) => {
                eprintln!(
                    "Failed to parse config file {}: {}",
                    &config_path.display(),
                    msg
                );
                std::process::exit(1);
            }
        },
        None => {
            warn!("No config file specified, using default config");
            Config::default()
        }
    };

    // validate config settings
    if let Some(dur) = &conf.server.shutdown_timeout {
        if dur.as_secs() > std::u16::MAX.into() {
            eprintln!("Maximum shutdown_timeout is {} seconds", std::u16::MAX);
            std::process::exit(1);
        }
    }

    conf
}

/// Main application config, typically read from a `.toml` file.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    /// Configuration for the application's HTTP server.
    #[serde(default)]
    pub server: ServerConfig,

    /// Configuration for connecting to Redis.
    #[serde(default)]
    pub redis: RedisConfig,

    /// Option list of queues to be created on application startup.
    pub queue: Option<HashMap<String, crate::models::queue::Settings>>,
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

    /// Get the Redis URL to use for connecting to a Redis server.
    pub fn redis_url(&self) -> &str {
        &self.redis.url
    }

    fn interpolate_env(raw_toml: &str) -> std::borrow::Cow<str> {
        let re = Regex::new(INTERPOLATE_RE)
            .expect("failed to compile interpolation regex");

        re.replace_all(raw_toml, |captures: &Captures| {
            let var_name = captures.get(1)
                .expect("capture should have at least 1 group");

            let value = match std::env::var(var_name.as_str()) {
                Ok(env_val) => env_val,
                Err(_) => captures.get(2).map_or_else(String::new, |v| v.as_str().to_owned())
            };

            value
        })
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

    /// Adds an artificial delay before returning to clients when a job is requested from an empty queue.
    /// Used to rate limit clients that might be excessively hitting the server, e.g. in tight loops.
    pub next_job_delay: Option<Duration>,

    /// Sets the application-wide log level.
    #[serde(deserialize_with = "deserialize_log_level")]
    pub log_level: log::Level,
}

fn deserialize_human_size<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let s: Option<&str> = Deserialize::deserialize(deserializer)?;
    Ok(match s {
        Some(s) => {
            let size: human_size::SpecificSize<human_size::Byte> = match s.parse() {
                Ok(size) => size,
                Err(_) => {
                    return Err(serde::de::Error::custom(format!(
                        "Unable to parse size '{}'",
                        s
                    )))
                }
            };
            Some(size.value() as usize)
        }
        None => None,
    })
}

fn deserialize_log_level<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<log::Level, D::Error> {
    let s: &str = Deserialize::deserialize(deserializer)?;
    match log::Level::from_str(s) {
        Ok(level) => Ok(level),
        Err(_) => Err(serde::de::Error::custom(format!(
            "Invalid log level: {}",
            s
        ))),
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

    /// Prefix added to internal Ocypod Redis keys. Avoids any key collisions in Ocypod is
    /// run on a Redis server used by other applications.
    pub key_namespace: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            url: "redis://127.0.0.1".to_owned(),
            key_namespace: "".to_owned(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_minimal() {
        let toml_str = r#"
[server]
host = "0.0.0.0"
port = 8023
log_level = "debug"

[redis]
url = "redis://ocypod-redis"
"#;
        let _: Config = toml::from_str(toml_str).unwrap();
    }

    #[test]
    fn parse_queues() {
        let toml_str = r#"
[server]
host = "::1"
port = 1234
log_level = "info"

[redis]
url = "redis://example.com:6379"

[queue.default]

[queue.another-queue]

[queue.a_3rd_queue]
timeout = "3m"
heartbeat_timeout = "90s"
expires_after = "90m"
retries = 4
retry_delays = ["10s", "1m", "5m"]
"#;
        let conf: Config = toml::from_str(toml_str).unwrap();
        let queues = conf.queue.unwrap();
        assert_eq!(queues.len(), 3);

        assert!(queues.contains_key("default"));
        assert!(queues.contains_key("another-queue"));

        let q3 = &queues["a_3rd_queue"];
        assert_eq!(q3.timeout, Duration::from_secs(180));
        assert_eq!(q3.heartbeat_timeout, Duration::from_secs(90));
        assert_eq!(q3.expires_after, Duration::from_secs(5400));
        assert_eq!(q3.retries, 4);
        assert_eq!(q3.retry_delays, vec![Duration::from_secs(10), Duration::from_secs(60), Duration::from_secs(300)]);
    }

    #[test]
    fn interpolation_regex_no_match() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        assert!(re.captures("").is_none());
        assert!(re.captures("foo").is_none());
        assert!(re.captures("{foo").is_none());
        assert!(re.captures("foo}").is_none());
        assert!(re.captures(" ").is_none());
        assert!(re.captures("").is_none());
        assert!(re.captures("${foo}").is_none());
        assert!(re.captures("${Foo}").is_none());
        assert!(re.captures("${123FOO}").is_none());
        assert!(re.captures("${A B C} ${D E F}").is_none());
    }

    #[test]
    fn interpolation_regex_match() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        let capture = re.captures("key = ${VALUE}").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "VALUE");

        let capture = re.captures("key = ${VA_LUE}").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "VA_LUE");

        let capture = re.captures("key = ${VA_LUE_123}").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "VA_LUE_123");

        let capture = re.captures("key = ${VALUE=default}").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "VALUE");
        assert_eq!(capture.get(2).unwrap().as_str(), "default");

        let capture = re.captures("key = ${VALUE=A longer (default) value}").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "VALUE");
        assert_eq!(capture.get(2).unwrap().as_str(), "A longer (default) value");

        let capture = re.captures("key = \"${FOO_1=true}, ${FOO_2=1}\"").unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "FOO_1");
        assert_eq!(capture.get(2).unwrap().as_str(), "true");
    }

    #[test]
    fn interpolation_regex_match_multiple() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        let captures: Vec<_> = re.captures_iter("${ONE=1} ${TWO=2}").into_iter().collect();
        assert_eq!(captures.len(), 2);

        let capture = captures.get(0).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "ONE");
        assert_eq!(capture.get(2).unwrap().as_str(), "1");

        let capture = captures.get(1).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "TWO");
        assert_eq!(capture.get(2).unwrap().as_str(), "2");
    }

    #[test]
    fn interpolation_regex_match_multiline() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        let conf = r#"
[server]
host = "::1"
port = ${OCYPOD_PORT=8023}
log_level = "${OCYPOD_LOG_LEVEL=info}"

[redis]
url = "redis://${REDIS_HOST}:${REDIS_PORT}"

[queue.default]

[queue.another-queue]

[queue.a_3rd_queue]
timeout = "${DEFAULT_QUEUE_TIMEOUT}"
heartbeat_timeout = "${DEFAULT_QUEUE_TIMEOUT}"
expires_after = "90m"
retries = 4
retry_delays = ["10s", "1m", "5m"]
        "#;
        let captures: Vec<_> = re.captures_iter(conf).into_iter().collect();
        assert_eq!(captures.len(), 6);

        let capture = captures.get(0).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "OCYPOD_PORT");
        assert_eq!(capture.get(2).unwrap().as_str(), "8023");

        let capture = captures.get(1).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "OCYPOD_LOG_LEVEL");
        assert_eq!(capture.get(2).unwrap().as_str(), "info");

        let capture = captures.get(2).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "REDIS_HOST");
        assert!(capture.get(2).is_none());

        let capture = captures.get(3).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "REDIS_PORT");
        assert!(capture.get(2).is_none());

        let capture = captures.get(4).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "DEFAULT_QUEUE_TIMEOUT");
        assert!(capture.get(2).is_none());

        let capture = captures.get(5).unwrap();
        assert_eq!(capture.get(1).unwrap().as_str(), "DEFAULT_QUEUE_TIMEOUT");
        assert!(capture.get(2).is_none());
    }

    #[test]
    fn interpolation_from_env_defaults() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        let conf = r#"
[server]
port = ${OCYTEST_OCYPOD_PORT=8023}
log_level = "${OCYTEST_OCYPOD_LOG_LEVEL=info}"

[redis]
url = "redis://${OCYTEST_REDIS_HOST=localhost}:${OCYTEST_REDIS_PORT=6379}"

[queue.${OCYTEST_QUEUE_PREFIX}foo]
        "#;

        let expected = r#"
[server]
port = 8023
log_level = "info"

[redis]
url = "redis://localhost:6379"

[queue.foo]
        "#;

        assert_eq!(Config::interpolate_env(conf), expected);
    }

    #[test]
    fn interpolation_from_env() {
        let re = Regex::new(INTERPOLATE_RE).unwrap();
        std::env::set_var("OCYTEST_B_OCYPOD_LOG_LEVEL", "debug");
        std::env::set_var("OCYTEST_B_REDIS_HOST", "example.com");
        std::env::set_var("OCYTEST_B_QUEUE_PREFIX", "prefix_");

        let conf = r#"
[server]
port = ${OCYTEST_B_OCYPOD_PORT=8023}
log_level = "${OCYTEST_B_OCYPOD_LOG_LEVEL=info}"

[redis]
url = "redis://${OCYTEST_B_REDIS_HOST=localhost}:${OCYTEST_B_REDIS_PORT=6379}"

[queue.${OCYTEST_B_QUEUE_PREFIX}foo]
        "#;

        let expected = r#"
[server]
port = 8023
log_level = "debug"

[redis]
url = "redis://example.com:6379"

[queue.prefix_foo]
        "#;

        assert_eq!(Config::interpolate_env(conf), expected);
    }
}