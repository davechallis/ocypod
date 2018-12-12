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
