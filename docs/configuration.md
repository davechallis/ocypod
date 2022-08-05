# Configuration

Ocypod is configured from a single [TOML](https://github.com/toml-lang/toml)
file.

All sections and fields of the configuration are optional, and defaults shown
will be used if not present.

The config file can be interpolated from the environment, see the
[environment variable interpolation](#environment-variable-interpolation)
section below for details.

## Server section

General configuration for the `ocypod-server` itself, uses `[server]` as a
section header.

Fields:

* `host` (string) - host address to listen on (default: "127.0.0.1")
* `port` (int) - port to listen on (default: 8023)
* `threads` (int) - number of HTTP worker threads (default: <number of CPUs>)
* `max_body_size` (string) - maximum body size for client POST/PUT requests as
  a human readable size (default: "256kB")
* `shutdown_timeout` (string) - graceful shutdown time for workers, triggered
  by SIGTERM signal (default: "30s")
* `timeout_check_interval` (string) - frequency of checks for jobs to time out,
  as a human readable duration (default: "30s")
* `retry_check_interval` (string) - frequency of checks for jobs to retry, as a
  human readable duration (default: "1m")
* `expiry_check_interval` (string) - frequency of checks for jobs to expire
  (i.e. remove from the queue system), as a human readable duration (default: "5m")
* `next_job_delay` (string) - artifical delay added to client responses when
  polling for new jobs (default: "0s")

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

Configuration for connectivity to the Redis server used by Ocypod. Uses
`[redis]` as a section header.

Fields:

* `url` (string) - [Redis connection URI](https://www.iana.org/assignments/uri-schemes/prov/redis) (default: "redis://127.0.0.1")
* `key_namespace` (string) - optional prefix to add to all Redis keys used by Ocypod(default: "")

The `key_namespace` can be set to avoid any key collisions when running separate Ocypod on a Redis
server used by other applications. The namespace will be added as a prefix to all keys Ocypod
creates (e.g. if `key_namespace="abc"`, then a queue named "x" will be created with key `abc:queue:x` instead of `queue:x`).

Example:

    [redis]
    url = "redis://:my_password@example.com:6379/my_db"

## Queue sections

Queues can be configured to be created when Ocypod starts by configuring them here, in order to simplify deployment without having to explicitly create queues via HTTP requests.

Configuring a queue here is the equivalent to calling the [PUT /queue/{queue_name}](api.md#put-queuequeue_name) endpoint.

Each queue section should be of the form:

    [queue.{queue_name}]

Note: queues configured in this way will be created at Ocypod startup if they don't exist, or updated if they exist with different settings. Queues will never be deleted if removed from a configuration file, that will remain a manual task.

Fields:

* `timeout` (string)
* `heartbeat_timeout` (string)
* `expires_after` (string)
* `retries` (integer)
* `retry_delays` (list of string)

For details on these, see the [queue settings](core_concepts.md#queue-settings) section.

Any of the fields may be omitted, in which case default values will be used.

Example:

The configuration below will create 3 queues, named `default`, `my_2nd_queue`, and `another-queue`.

    [queue.default]

    [queue.my_2nd_queue]
    retries = 5
    retry_delays = ["10s", "1m", "5m"]

    [queue.another-queue]
    timeout = "5m"
    heartbeat_timeout = "30s"
    expires_after = "1d"

## Environment variable interpolation

The TOML configuration file supports interpolation from environment variables,
in order to simplify running Ocypod in different environments without having
to modify the configuration file each time.

Interpolation can be done anywhere in the config file, and is of the form:

```
${VARIABLE_NAME}
```

If an environment variable is not set, then the service will fail to start
with an error.

Defaults can also be specified to allow fallback values if an environment
variable is not set:

```
${VARIABLE_NAME=default_value}
```

An empty default value can also be used as a default to allow an environment
variable to be missing and default to an empty string, e.g.:

```
name = "${PREFIX=}name"
```


An example of a config file using these features might look as follows:

```toml
[server]
host = "${OCYPOD_HOST=localhost}"
port = ${OCYPOD_PORT=8023}
log_level = "${OCYPOD_LOG_LEVEL=info}"
timeout_check_interval = "5s"
retry_check_interval = "5s"
expiry_check_interval = "5s"

[redis]
url = "redis://${REDIS_HOST}:${REDIS_PORT}"

[queue.${QUEUE_PREFIX=}default}]
timeout = "1m"
retries = 2

[queue.${QUEUE_PREFIX=}queue2}]
timeout = "5m"
```

This would require the `REDIS_HOST` and `REDIS_PORT` environment variables to
be set, and would fall back to defaults for the others.
