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
