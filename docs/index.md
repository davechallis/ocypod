# Ocypod

Ocypod is a language-agnostic, Redis-backed job queue server with
an easy to use HTTP interface. Its focus is on handling and monitoring
long running jobs.

## Features

* simple setup - only requirement is Redis
* language agnostic - uses HTTP/JSON protocol, clients/workers can be
  implemented in any language
* long running jobs - handle jobs that may be running for hours/days,
  detect failure early using heartbeats
* simple HTTP interface - no complex binary protocols or client/worker logic
* flexible job metadata - allows for different patterns of use (e.g. progress
  tracking, partial results, etc.)
* job inspection - check the status of any jobs submitted to the system
* tagging - custom tags allow easy grouping and searching of related jobs
* automatic retries - re-queue jobs on failure or timeout

## Installation

### Prerequisites

#### Redis

Ocypod uses [Redis](https://redis.io/) as a storage backend, and it's
recommended to use a dedicated Redis instance (or at least its own DB) to
avoid any clashes with other key spaces.

The latest [stable release](https://redis.io/download) (5.0) is recommended,
though there are no known issues using 4.0 or 3.2.

For development purposes, you can use `docker-compose up` in order to run a local redis instance.

### Building from source

Ocypod is written in [Rust](https://www.rust-lang.org/en-US/), so you'll need a [Rust installation](https://www.rust-lang.org/en-US/install.html) to build from source. Ocypod is generally built/tested using the latest stable Rust compiler release.

To build:

    $ git clone https://github.com/davechallis/ocypod.git
    $ cd ocypod
    $ cargo build --release

Check built executable:

    $ ./target/release/ocypod-server --version

## Quickstart

This brief tutorial will run you through:

1. creating a new queue
2. adding a job to a queue
3. fetching a job to work on
4. checking the status of a job
5. completing a job

### Start Ocypod and check connectivity

Make sure Redis is running locally, the start the Ocypod server using:

    $ ocypod-server

This will start a new server running, listening on port 8023 and connecting
to Redis running on `127.0.0.1:6379`. Ocypod would typically be started using
a configuration file, see the [Configuration](#configuration) section for
for details.

Ocypod uses an HTTP interface, so we'll use [curl](https://curl.haxx.se/)
command for this section, but feel free to use any HTTP client that you prefer.

Now that the server is running, check that it's healthy and able to connect
to Redis:

    $ curl localhost:8023/health
    {"status":"healthy"}

### Create a new queue

All jobs we create are placed on a queue, so we'll create a new queue by
sending a JSON map containing queue settings to the server.

The following will create a new queue named "demo" with a job
timeout of 10 minutes, and everything else as default:

    $ curl -i -H 'content-type: application/json' -XPUT -d '{"timeout": "10m"}' localhost:8023/queue/demo
    
    HTTP/1.1 201 Created
    location: /queue/demo
    content-length: 0
    date: Wed, 05 Dec 2018 15:36:32 GMT
    
We should now have a queue named "demo", which will time out any jobs that don't
complete in 10 minutes by default (though this can be overidden on a per-job
basis).
    
Next, verify that the newly created queue exists, and see its full configuration:

    $ curl localhost:8023/queue/example
    {"timeout":"10m",
     "heartbeat_timeout":"0s",
     "expires_after":"5m",
     "retries":0,
     "retry_delays":[]}
 
These can be changed at any time by re-running the queue creation command - if the
queue already exists, its settings will be updated, and a 204 response code returned
on success.
     
For a full list of queue settings, see the [Queue settings](#queue-settings)
section.

### Adding a job to the queue

In a similar manner to the above, we can add a job to the queue by sending
a JSON definition of the job to the server.

The "input" fields contains any JSON that you want, this is the main job
payload that workers will receive and process. This is also where you'd set
other job configuration, such as number of retries, timeouts, etc. (see the
TODO: job creation section for more details).

    $ curl -i -H 'content-type: application/json' -XPOST -d '{"input": [1,2,3]}' localhost:8023/queue/demo/job
      
    HTTP/1.1 201 Created
    content-length: 1
    location: /job/1
    content-type: application/json
    date: Wed, 05 Dec 2018 15:39:31 GMT
     
    1

This creates a new job in the queue, with an auto-generated job ID (1 in this
case). You can use this ID to check the job's status at the job's URL:

    $ curl localhost:8023/job/1/status
    "queued"

Or you can get the all job-related fields as JSON:

	$ curl localhost:8023/job/1
	{"id":1,
	 "queue":"demo",
	 "status":"queued",
	 "tags":null,
	 "created_at":"2018-12-05T15:39:31.841731Z",
	 "started_at":null,
	 "ended_at":null,
	 "last_heartbeat":null,
	 "input":[1,2,3],
	 "output":null,
	 "timeout":"10m",
	 "heartbeat_timeout":"1m",
	 "expires_after":"5m",
	 "retries":3,
	 "retries_attempted":0,
	 "retry_delays":null,
	 "ended":false}

A detailed explanationof these fields can be found in the
[job metadata](#job-metadata) section.

### Start and complete a job

Next, we'll simulate the role of a worker, and ask for a job, update some job
data, then mark the job as completed.

Ask the "demo" queue for a job:

	$ curl localhost:8023/queue/demo/job
	{"id":1,"input":[1,2,3]}
	
As you can see, we receive the "input" payload that was added at job creation
time.

Mark the job as complete and write some results:

    $ curl -i -XPATCH -H 'content-type: application/json' -d '{"status": "completed", "output": [2,4,6]}' localhost:8023/job/1
    HTTP/1.1 204 No Content
    date: Wed, 05 Dec 2018 16:24:04 GMT

The `output` field can be updated at any time while a job is running, and is
typically used to store job results, progress information, errors, etc.

You can then check the job's status and get its output (as clients might typically
to in order to track a job's progress):

    $ curl -i 'http://localhost:8023/job/61?fields=status,ended_at,output'
    {"status": "completed",
     "ended_at": "2018-12-05T16:24:08.4385734Z",
     "output": [2,4,6]}

Since the job we created has `expires_after` set to `5m`, this job will
be removed from the queue server 5 minutes after its `ended_at` date/time.


## Configuration

Ocypod is configured from a single [TOML](https://github.com/toml-lang/toml) file.

All sections and fields of the configuration are optional, and defaults shown will be used if not present.

### Server section ###

General configuration for the `ocypod-server` itself, uses `[server]` as a section header.

Fields:

* `host` (string) - host address to listen on (default: "127.0.0.1")
* `port` (int) - port to listen on (default: 8023)
* `threads` (int) - number of HTTP worker threads (default: <number of CPUs>)
* `max_body_size` (string) - maximum body size for client POST/PUT requests as a human readable size (default: "256kB")
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

### Redis section

Configuration for connectivity to the Redis server used by Ocypod. Uses `[redis]` as a section header.

Fields:

* `url` (string) - [Redis connection URI](https://www.iana.org/assignments/uri-schemes/prov/redis) (default: "redis://127.0.0.1")
* `threads` (int) - number of workers handling connections to Redis (default: <number of CPUs>)

Example:

    [redis]
    url = "redis://:my_password@example.com:6379/my_db"
    threads = 1

## Core concepts

### Job

A job in Ocypod represents some task created by clients, and to be completed
by workers.

Each job has a set of metadata associated with it, some of which is managed
by Ocypod, and some of which can be created/updated by clients/workers.

#### Job lifecycle and statuses

When a job is initially created, it's added to a queue, assigned the
`created` status.

Clients will then poll that queue for new jobs, receiving the job's payload
(the contents of its `input` field), and the job's ID. The job is removed
from its queue, and its status is set to `running`.

If the client completes the job, it will send a message to ocypod asking
it to update the job's status to `completed`. If there's some
error/exception and the client can't finish the job, it will mark the job
as `failed`.

If the client fails to complete/fail a job before the job's timeout (or heartbeat timeout) is exceeded, then
ocypod marks the job as `timed_out`.

Ocypod will periodically look at all failed and timed out jobs and check if they're elgible for automatic retries,
and if so, will re-queue them.


TODO: link to job API
TODO: link to job creation

#### Job metadata

The ocypod server maintains the following information about a job, some of which is immutable, some of which will be modified by ocypod throughout a job's lifecycle, and some if modifiable by user request.

* `id` - autogenerated ID for the job, created when a job is first created and queued
* `queue` - name of the queue the job was created in
* `status` - current status of the job
* `tags` - list of tags (if any) assigned to this job at creation time
* `created_at` - date/time this job was first created and queued
* `started_at` - date/time this job was accepted by a client, and the job's status changed to `running`
* `ended_at` - date/time this job stopped running, whether due to successful completed, timing out, or failure
* `last_heartbeat` - date/time the last heartbeat for this job was sent by the client executing it
* `input` - the job's payload, sent by the client creating this job - this typically contains the data needed for a worker to execute the job
* `output` - contains any information the client working on this job decides to store here, this might include the job's result, progress information, partial results, etc. - it can be set anytime the task is running
* `timeout` - maximum execution time of the job before it's marked as timed out
* `heartbeat_timeout` - maximum time without receiving a heartbeat before the job is marked as timed out
* `expires_after` - amount of time this job metadata will persist in ocypod after the job reaches a final state (i.e. `completed`/`failed`/`timed_out` with no retries remaining)
* `retries` - number of times this job will automatically be requeued on failure
* `retries_attempted` - number of times this job has failed and been requeued
* `retry_delays` - minimum amount of time to wait between each retry attempt
* `ended` - indicates whether the job is in a final state or not (i.e. completed, or failed/timed out with no retries remaining)


### Job Status

A job in Ocypod will always have one of the following statuses:

* `created` - set by the server when a job is first created and added to a queue
* `running` - set by the server when a worker picks up a job
* `completed` - set by the client to mark a job as successfully completed
* `failed` - set by the client to mark a job as having failed
* `timed_out` - set by the server when a job exceeds either its `timeout` or `heartbeat_timeout`
* `cancelled` - set by client to mark that a job has been cancelled

To aid clients that are checking on the status of jobs, each job also has an `ended`
boolean field. This is set to `true` if the job is in its final state, or `false` otherwise.

A job is marked as ended in the following circumstances:

* job has `completed` status
* job has `cancelled` status
* job has `failed` status and 0 retries remaining
* job has `timed_out` status and 0 retries remaining

### Queue

Each queue in Ocypod has its own settings, which are generally used as
defaults for jobs created on that queue (they can be overridden on a per-job
basis though).

A queue in Ocypod is a [FIFO](https://en.wikipedia.org/wiki/FIFO_(computing_and_electronics)),
with new jobs being added to the beginning of the queue, and workers
taking jobs from the end of the queue.


#### Queue settings

TODO: link to API doc for getting queue info

Each queue has a number of settings, which are defaults that are applied
to new jobs created in that queue. Each can be overridden on a per-job basis,
they just exist at the queue level for convenience.

---

##### `timeout`

This is the maximum amount of time a job can be running for before it's
considered to have timed out. It's specified as a human readable duration
string, e.g. "30s", "1h15m5s", "3w2d", etc.

To disable timeouts entirely, this can be set to "0s".

---

##### `heartbeat_timeout`

For long running jobs, it's recommended that workers send regular heartbeats
to the ocypod server to let it know that the job is still being processed.
This allows timeouts or failures to be noticed much earlier than if just
relying on `timeout`.

The `heartbeat_timeout` setting determines how long a job can be running
without getting a heartbeat update before it's considered to have timed out.
It's specified as a human readable duration string.

To disable heartbeat timeouts entirely, this can be set to "0s".

---

##### `expires_after`

This setting determines how long jobs that have ended (either successfully
completed, failed, or timed out without any retries) will remain in the system. After this period of time, the job and its metadata will be cleared from ocypod.

This is specified as a human readable duration string, and can be set to
"0s" to disable expiry entirely. In this case, you'll be responsible for
managing and cleaning up old jobs manually.

### Tag

A tag is a short string that can be attached to a job at creation time.
An endpoint for getting all job IDs by tag is provided.

This allows separate jobs to be grouped together, use cases include e.g.:

* using a batch ID tag to a related set of jobs
* using a username tag to track all jobs belonging to a user
* using a source tag to track the client/process that created a job

## Writing a worker

One of the goals of Ocypod is to make writing clients/workers a
 straightforward process, since all communication takes place using HTTP.
It's also designed to be fairly flexible, to allow for different
patterns/workflows.

The simplest possible worker would generally do the following in a loop:

1. poll a queue (or queues) for a job to work on
2. start processing a job
3. when job is complete, update the job's status to `completed`

A more fully featured client might do the following:

1. poll a queue (or queues) for a job to work on
2. start processing a job
3. send regular job heartbeat to let ocypod know the worker is alive
4. update the job's `output` field with progress information
5. when job is complete, update the job's status to `completed`, and update it's `output` to contain the final result

It's generally assumed that a single client/worker will be updating a job's
status, output, etc., but that any number of clients might be reading its
metadata (to e.g. update a progress page, to log some statistics, to
check for completion, etc.).


## Full API

TODO: link to swagger spec

### Queue endpoints

Used for interacting with queues.

---

#### `GET /queue`

Get a JSON list of all known queues.

##### Returns

* 200 - JSON list of strings

##### Example

    $ curl localhost:8023/queue
    ["my-queue", "another_queue", "queue3"]

---

#### `GET /queue/{queue_name}`

Get settings for given queue name.

##### Returns

* 200 - JSON object containing queue settings
* 400 - invalid queue name passed as parameter
* 404 - no queue with given name was found

##### Example

    $ curl localhost:8023/queue/example
    {"timeout":"10m","heartbeat_timeout":"5m","expires_after":"5m"}

---

#### `PUT /queue/{queue_name}`

Create new queue or update existing queue with given settings.

##### Request

The request body must contain JSON of the form:

    {"timeout": <duration>, "heartbeat_timeout": <duration>, "expires_after": <duration>}

where `<duration>` is a human readable string of the form `"30s"`, `"5m"`, `"1w2d7h"`, etc.

Any of the fields may be omitted (e.g. `{}` is the minimum accepted input), in which case server-wide default values will be used.

Any of the fields may be set to "0s" to disable timeout/expiry.

##### Returns

* 201 - new queue created
* 204 - existing queue updated
* 400 - invalid queue name or queue settings given

##### Example

    $ curl -i -H 'content-type: application/json' -XPUT -d '{"timeout": "10m"}' localhost:8023/queue/example
    HTTP/1.1 201 Created
    location: /queue/example
    content-length: 0
    date: Fri, 16 Nov 2018 17:47:55 GMT

---

#### `DELETE /queue/{queue_name}`

Delete an existing queue, and any jobs still queued on it. Any running or ended jobs won't be affected.

If any running jobs created on this queue have retries remaining, they'll instead remain in their failed/timed out state, and be eligible for expiry if they can't be requeued due to their original queue no longer existing.

##### Returns

* 204 - existing queue successfully deleted
* 400 - invalid queue name given
* 404 - queue with given name not found

##### Example

    $ curl -i -XDELETE localhost:8023/queue/example
    HTTP/1.1 204 Queue deleted
    date: Tue, 20 Nov 2018 18:07:47 GMT

---

#### `GET /queue/{queue_name}/job`

Get the next job to work from the given queue, if any, as a JSON job payload.

This JSON payload is of the form:

    {"id": <integer>,
     "input": <any JSON>}

where `id` is a unique ID that's autogenerated by Ocypod when a job is created, and `input` is any JSON provided by the client that created the job.

When a client gets a job in this way, the job is marked as running, and it's removed from the queue.

##### Returns

* 200 - JSON payload as described above
* 204 - no jobs available in this queue
* 400 - invalid queue name given
* 404 - queue with given name not found

##### Example

    $ curl -i localhost:8023/queue/example/job
    HTTP/1.1 200 OK
    content-length: 58
    content-type: application/json
    date: Tue, 20 Nov 2018 18:52:56 GMT

    {"id":77,"input":{"some_key": [1, 2, 3]}}

-----

#### `POST /queue/{queue_name}/job`

Create a new job on given queue using given JSON.

##### Request

The request body must contain JSON of the form:

    {"input": <any JSON>,
     "tags": <list of strings>,
     "timeout": <duration>,
     "heartbeat_timeout": <duration>,
     "expires_after": <duration>,
     "retries": <integer>,
     "retry_delays": <list of durations>}

All fields are optional (so `{}` is the minimum valid job that can be created).

`input` represents the job's main payload, this is what clients that accept the job will use to work on. This will typically contain input data to process, and can be any simple or complex JSON value. Defaults to `null` if not specified.

`tags` are an optional list of strings to attach to this job. These can be used to look up jobs by tag using the `/tag` endpoints. These can be used to e.g. attach a username to jobs, or to attach a batch ID to a number of related job created by the same process. Defaults to `[]` if not specified.

`timeout` is the maximum amount of time the job can run before it's marked as timed out. Default is to use the queue's setting.

`heartbeat_timeout` is the maximum amount of time between heartbeats before the job is marked as timed out. Default is to use the queue's setting.

`expires_after` is the amount of time the job and its metadata will remain in ocypod after the job completes, fails, or times out (with no retries remaining). Default is to use the queue's setting.

`retries` is the number of times to re-queue this job on failure, so that it can be picked up and tried again. Default is to use the queue's setting.

`retry_delays` is a list of durations specifying the minimum time to wait before re-queuing a job on failure. This can be used to specify linear/exponential/any backoff intervals when retrying jobs.

If the number of retries exceeds the length of this list, then the last value will always be used, e.g. with `retries: 4` and `retry_delays: ["10s", "1m", "5m"]`, the job won't be retried for at least 10 seconds after the 1st failure, for 1 minute after the 2nd failure, for 5 minutes after the 3rd failure, and for 5 minutes on the 4th failure.

##### Returns

201 - job successfully created, response contains ID of new job, and location of job in `location` header
400 - invalid queue name or invalid job creation JSON given
404 - queue with given name not found

### `/job` endpoints

Endpoints for interacting with jobs in any state.

---

#### `GET /job/{job_id}[?fields=<comma separated list of fields>]`

Get metadata about given job as a JSON object.

Optionally accepts a comma separated list of fields to fetch, otherwise will get all fields by default.

Response JSON is an object with each field being a key, i.e.

     {<field1>: <value>, <field2>: <value>, ... }

##### Returns

* 200 - JSON object containing requested fields
* 400 - unable to parse list of fields given, or unrecognised field given
* 404 - job with given ID does not exist

##### Example

    $ curl 'localhost:8023/job/77?fields=id,status,started_at,ended_at'
    {"id": 77,
     "status": "running",
     "started_at": "2018-11-20T18:52:42.700853Z",
     "ended_at": null}

---

#### `PATCH /job/{job_id}`

Clients can use this to modify a job's status, and/or its `output` field.

This endpoint is generally used to mark a job as completed/failed/cancelled, to write progress/error information to `output`, and to write final results to `output`.

##### Request

The request must be JSON of the form:

    {"status": ("completed"|"failed"|"cancelled"),
     "output": <any JSON>}

All fields are optional, only fields that are present will cause any changes.

##### Response

* 204 - job successfully updated
* 400 - invalid or no JSON sent
* 404 - no job with given ID exists


##### Example

Mark a job as completed, and write its results:

    $ curl -i -XPATCH -H 'content-type: application/json' -d '{"status": "completed", "output": {"result": 321, "progress": 100.0, "warnings": 3}}' localhost:8023/job/23
    HTTP/1.1 204 OK
    date: Wed, 21 Nov 2018 11:13:34 GMT

---

#### `DELETE /job/{job_id}`

Delete a job from ocypod, regardless of its state. Any jobs which have ended and have a non-zero `expires_after` field will eventually be cleaned up once they've ended. This endpoint is available if this is something you want to control manually, or to remove jobs that are still queued.

While you can delete jobs that are currently running, you'll need to make sure that clients/workers gracefully handle this case. TODO: something about using cancelled

##### Response

* 204 - job successfully deleted
* 404 - job with given ID does not exist

##### Example

    $ curl -XDELETE localhost:8023/job/123
    HTTP/1.1 204 OK
    date: Wed, 21 Nov 2018 11:13:34 GMT

---

#### `PUT /job/{job_id}/heartbeat`

Used by clients that are working on a job to send a heartbeat for it. This is used to let the server know that the job is still actively being worked on.

This is only useful for jobs with a non-zero `heartbeat_timeout` set, and can only be used on jobs that are running.

##### Response

* 204 - heartbeat successfully updated
* 404 - job with given ID does not exist
* 409 - job is not in a state where heartbeat can be updated (e.g. job is queued/failed/cancelled etc.)

##### Example

    $ curl -XPUT localhost:8023/job/23/heartbeat
    HTTP/1.1 204 OK
    date: Wed, 21 Nov 2018 11:13:34 GMT

---

#### `GET /job/{job_id}/output`

Get a job's `output` JSON field.

##### Response

200 - JSON contents of the job's `output` field
404 - job with given ID does not exist

##### Example

    $ curl localhost:8023/job/123/output
    {"a": 1, "b": [2, 3, 4]}

---

#### `PUT /job/{job_id}/output`

Set a job's `output` field to given JSON. This has the same effect as updating using the `PATCH /job/{job_id}` endpoint and setting the `output` field in the request JSON, but is more convenient if only updating this field.

##### Response

* 204 - job's output field successfully set
* 404 - job with given ID does not exist
* 409 - job is not in a valid state

##### Example

    $ curl -i -XPUT -H 'content-type: application/json' -d '[1,2,3]' localhost:8023/job/12/output
    HTTP/1.1 204 OK
    date: Wed, 21 Nov 2018 11:13:34 GMT

---

### `/tag` endpoints

Information about jobs that were created with tags can be retreived here. Tags are typically used as a way of grouping multiple jobs in some way, and then later retrieving jobs by tag.

Use cases include e.g.:

* using a "username" tag, to track which users have submitted which jobs
* using a "batch ID" tag, to track jobs related to an ETL run or other batch process
* using a "host" tag, to track which hosts created jobs

---

#### `GET /tag/{tag_name}`

Get a JSON list of job IDs with given tag. If no jobs have the requested tag, or if the tag doesn't exist, then an empty list will be returned.

##### Response

* 200 - JSON list of job IDs
* 400 - invalid tag name requested

##### Example

    $ curl localhost:8023/tag/user3
    [17, 19, 225]

---

### `/info` endpoints

These provide information about the Ocypod system as a whole.

#### `GET /info`
#### `GET /info/version`

### `/health` endpoints

Provides a way of checking the health of the Ocypod server. Provides a JSON response containing health of the system, and optionally a message about any error detected.

#### `GET /health`

Get JSON indicating the health of the Ocypod server. Currently this checks whether it can successfully connect to and communicate with Redis.

Returns JSON of the form:

    {"status": ("healthy"|"unhealthy"), "error": <error message>}

The `error` field is optionally present if they status is "unhealthy", the "status" field is always present.

##### Returns

* 200 - health check completed


## Internal design

The focus of this server was to handle a common way for multiple languages/services to deal with background jobs in a common way, especially for dealing with jobs that may take hours/days to run.

Designing for the very high throughput low-latency use case (e.g. tens of thousands of workers, millions of jobs per day) was not a focus for this project, there are plenty of good systems out there which deal with that very well.

Ocypod makes heavy use of [Redis transactions](https://redis.io/topics/transactions), in order to allow clients/workers to modify jobs as they are in progress, while ensuring the integrity of the system.

The `ocypod-server` itself is stateless, so there should theoretically be no issues with running multiple instances of the server with the same Redis instance as a backend.

Internally, several Redis data structures are used:

* `limbo` - list used as temporary holding area for jobs that are popped from a queue, they're kept here while the job's metadata is updated, before being moved to another queue
* `running` - list storing currently running job IDs
* `failed` - list storing failed or timed out job IDs
* `ended` - list storing IDs of jobs that have reached a final state (i.e. completed, cancelled, or failed/timed out with no retries remaining)
* `job_id` - counter used to autogenerate job IDs
* `stats:{statistic}` - used to store global statistics
* `tag:{name}` - used to index job IDs with given tag name
* `job:{job_id}` - hash containing a single jobs metadata
* `queue:{queue_name}` - hash containing a queue's settings
* `queue:{queue_name}:jobs` - list containing queued job IDs, used as a FIFO

The ocypod-server runs three background tasks which monitor different queues and modify job state as necessary:

* timeout check - checks all jobs in the `running` queue for timeouts or heartbeat times, and moves them to the `failed` queue as necessary
* retry check - checks all jobs in the `failed` queue for retry eligibility, and re-queues them on their original queue if elibible, otherwise this moves them to the `ended` queue
* expiry check - checks all jobs in the `ended` queue for expiry, removing the job from Redis entirely
