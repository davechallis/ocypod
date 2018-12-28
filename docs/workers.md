# Writing a worker

One of the goals of Ocypod is to make writing clients/workers a
 straightforward process, since all communication takes place using HTTP/JSON.

The simplest possible worker would generally do the following in a loop:

1. poll a queue (or queues) for a job to work on
2. start processing a job
3. when job is complete, update the job's status to `completed`

A more fully featured client might do the following:

1. poll a queue (or queues) for a job to work on
2. start processing a job
3. send regular job heartbeat to let Ocypod know the worker is alive
4. update the job's `output` field with progress information
5. when job is complete, update the job's status to `completed`, and update it's `output` to contain the final result

It's generally assumed that a single client/worker will be updating a job's
status, output, etc., but that any number of clients might be reading its
metadata (to e.g. update a progress page, to log some statistics, to
check for completion, etc.).
