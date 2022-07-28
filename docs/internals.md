# Internal design

The focus of this server was to handle a common way for multiple
languages/services to deal with background jobs in a common way, especially
for dealing with jobs that may take hours/days to run.

Designing for the very high throughput low-latency use case (e.g. tens of
thousands of workers, millions of jobs per day) was not a focus for this
project, there are plenty of good systems out there which deal with that very
well.

Ocypod makes heavy use of [Redis transactions](https://redis.io/topics/transactions),
in order to allow clients/workers to modify jobs as they are in progress,
while ensuring the integrity of the system.

The `ocypod-server` itself is stateless, so there should be no issues with
running multiple instances of the server with the same Redis instance as a
backend.

Internally, several Redis data structures are used, using the following names in Redis (unless altered by the `key_namespace` parameter):

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

The ocypod-server runs three background tasks which monitor different queues
and modify job state as necessary:

* timeout check - checks all jobs in the `running` queue for timeouts or heartbeat times, and moves them to the `failed` queue as necessary
* retry check - checks all jobs in the `failed` queue for retry eligibility, and re-queues them on their original queue if elibible, otherwise this moves them to the `ended` queue
* expiry check - checks all jobs in the `ended` queue for expiry, removing the job from Redis entirely