# Unreleased

* Updated to use Actix web 4.x (beta), Tokio 1.x.
* Switch to using deadpool to manage async Redis connections, to avoid possible
  race conditions with transactions over a multiplexed connection (see
  [#26](https://github.com/davechallis/ocypod/issues/26).
* More consistent HTTP status codes returned on error.

# 0.6.2 (2021-09-10)

* Fix bug where requesting "ended" without one of the fields it depended on caused an error.

# 0.6.1 (2021-08-31)

* Fix bug where calculation of derived "ended" field did not take into account
  number of retries attempted.

# 0.6.0 (2020-12-11)

* Add optional configuration sections for queues, simplifying deployment by allowing them to be created when Ocypod starts.
* Allow `redis` and `server` sections of configuration to optional in configuration, using default values if not specified.

# 0.5.0 (2020-08-27)

* Large internal refactoring to move to using async/await throughout.
* Migrated from using Actix web 1.0 to 2.0.
* Moved to using async Redis connection, and connection manager.
* Removed use of Actix actors.
* Redis threads configuration option removed.

# 0.4.0 (2019-07-05)

* Added `/queue/{name}/job_ids` endpoint to return job IDs by status for a
  given queue.
* Fix to `/queue/{name}/size` endpoint returning 404.
* Fix to incorrect endpoint handler mapping for `/job/{id}/output` endpoint.

# 0.3.0 (2019-06-28)

* Migrated codebase to use actix-web 1.0.

# 0.2.0 (2018-12-17)

* Migrated to Rust 2018 edition.
* Added graceful shutdown timeout parameter.
* Added queue size endpoint.
* Default to setting number of Redis workers to number of CPUs.
* Return JSON from version endpoint for consistency.

# 0.1.0 (2018-12-06)

Initial release.
