# Configuration

Ocypod is configured from a single [TOML](https://github.com/toml-lang/toml) file.

All sections and fields of the configuration are optional, and defaults shown will be used if not present.

## Server section ###

General configuration for the `ocypod-server` itself, uses `[server]` as a section header.

Fields:

* `host` (string) - host address to listen on (default: "127.0.0.1")
* `port` (int) - port to listen on (default: 8023)
* `threads` (int) - number of HTTP worker threads (default: <number of CPUs>)
* `max_body_size` (string) - maximum body size for client POST/PUT requests as a human readable size (default: "256kB")
* `shutdown_timeout` (string) - graceful shutdown time for workers, triggered by SIGTERM signal (default: "30s")
* `timeout_check_interval` (string) - frequency of checks for jobs to time out, as a human readable duration (default: "30s")
* `retry_check_interval` (string) - frequency of checks for jobs to retry, as a human readable duration (default: "1m")
* `expiry_check_interval` (string) - frequency of checks for jobs to expire (i.e. remove from the queue system), as a human readable duration (default: "5m")
* `next_job_delay` (string) - artifical delay added to client responses when polling for new jobs (default: "0s")

Example:

    [server]
    host = "0.0.0.0"
    port = 8023
    threads = 2
    max_body_size = "10MiB"
    timeout_check_interval = "1m"
    retry_check_interval = "30s"
    expiry_check_interval = "1h"
    next_job_delay = "5s"

## Redis section

Configuration for connectivity to the Redis server used by Ocypod. Uses `[redis]` as a section header.

Fields:

* `url` (string) - [Redis connection URI](https://www.iana.org/assignments/uri-schemes/prov/redis) (default: "redis://127.0.0.1")
* `threads` (int) - number of workers handling connections to Redis (default: <number of CPUs>)

Example:

    [redis]
    url = "redis://:my_password@example.com:6379/my_db"
    threads = 1
