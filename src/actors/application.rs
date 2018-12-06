//! Actor and message definitions for interacting with the Ocypod queue application.
use std::thread;
use std::time::Duration;

use actix::prelude::*;
use serde_json;
use redis;

use crate::application::RedisManager;
use crate::models::{job, queue, OcyResult, ServerInfo};

/// Main actor that exposes job queue functionality.
///
/// Most HTTP request handlers will construct messages and send to this actor to create queues/jobs, get jobs, etc.
pub struct ApplicationActor {
    client: redis::Client,
    conn: Option<redis::Connection>,
}

impl Actor for ApplicationActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!("Ocypod application worker started");
    }
}

impl ApplicationActor {
    /// Create a new RedisActor which connects to Redis using given client.
    pub fn new(client: redis::Client) -> Self {
        ApplicationActor { client, conn: None }
    }

    fn get_connection(&mut self) -> OcyResult<&redis::Connection> {
        if self.is_connection_valid() {
            return Ok(self.conn.as_ref().unwrap());
        }

        self.init_connection()
    }

    fn is_connection_valid(&self) -> bool {
        match self.conn {
            Some(ref conn) => {
                match redis::cmd("PING").query(conn) {
                    Ok(()) => true,
                    Err(err) => {
                        warn!("Redis PING failed: {}", err);
                        false
                    }
                }
            }
            None => false,
        }
    }

    // TODO: configurable timeouts, retrys, backoff, etc.
    fn init_connection(&mut self) -> OcyResult<&redis::Connection> {
        loop {
            match self.client.get_connection() {
                Ok(conn) => {
                    debug!("Established new Redis connection");
                    self.conn = Some(conn);
                    return Ok(self.conn.as_ref().unwrap());
                },
                Err(err) => {
                    warn!("Redis connection failed, retrying in 1s: {}", err);
                    thread::sleep(Duration::from_secs(5));
                }
            }
        }
    }
}


/// Message used to get a list of all Queue names.
pub struct GetQueueNames;

impl Message for GetQueueNames {
    type Result = OcyResult<Vec<String>>;
}

impl Handler<GetQueueNames> for ApplicationActor {
    type Result = OcyResult<Vec<String>>;

    fn handle(&mut self, _msg: GetQueueNames, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).queue_names()
    }
}


/// Message used to get all job IDs that have given tag.
pub struct GetTaggedJobs(pub String);

impl Message for GetTaggedJobs {
    type Result = OcyResult<Vec<u64>>;
}

impl Handler<GetTaggedJobs> for ApplicationActor {
    type Result = OcyResult<Vec<u64>>;

    fn handle(&mut self, msg: GetTaggedJobs, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).tagged_job_ids(&msg.0)
    }
}


/// Message used to create or update a queue with given name and settings.
pub struct CreateOrUpdateQueue(pub String, pub queue::Settings);

impl Message for CreateOrUpdateQueue {
    type Result = OcyResult<bool>;
}

impl Handler<CreateOrUpdateQueue> for ApplicationActor {
    type Result = OcyResult<bool>;

    fn handle(&mut self, msg: CreateOrUpdateQueue, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).create_or_update_queue(&msg.0, &msg.1)
    }
}


/// Messaged used to delete a queue with given name.
pub struct DeleteQueue(pub String);

impl Message for DeleteQueue {
    type Result = OcyResult<bool>;
}

impl Handler<DeleteQueue> for ApplicationActor {
    type Result = OcyResult<bool>;

    fn handle(&mut self, msg: DeleteQueue, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).delete_queue(&msg.0)
    }
}


/// Messaged used to get summary information about a queue with given name.
pub struct GetQueueSettings(pub String);

impl Message for GetQueueSettings {
    type Result = OcyResult<queue::Settings>;
}

impl Handler<GetQueueSettings> for ApplicationActor {
    type Result = OcyResult<queue::Settings>;

    fn handle(&mut self, msg: GetQueueSettings, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).queue_settings(&msg.0)
    }
}


/// Message used to create a new job on given queue with given job specification.
pub struct CreateJob(pub String, pub job::CreateRequest);

impl Message for CreateJob {
    type Result = OcyResult<u64>;
}

impl Handler<CreateJob> for ApplicationActor {
    type Result = OcyResult<u64>;

    fn handle(&mut self, msg: CreateJob, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).create_job(&msg.0, &msg.1)
    }
}


/// Message used to ask for the next job on given queue.
pub struct NextJob(pub String);

impl Message for NextJob {
    type Result = OcyResult<Option<job::Payload>>;
}

impl Handler<NextJob> for ApplicationActor {
    type Result = OcyResult<Option<job::Payload>>;

    fn handle(&mut self, msg: NextJob, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).next_queued_job(&msg.0)
    }
}


/// Message used to ask for overall summary information about the application.
pub struct GetInfo;

impl Message for GetInfo {
    type Result = OcyResult<ServerInfo>;
}

impl Handler<GetInfo> for ApplicationActor {
    type Result = OcyResult<ServerInfo>;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).server_info()
    }
}


/// Message used to ask for summary info about given job ID.
pub struct GetJobFields(pub u64, pub Option<Vec<job::Field>>);

impl Message for GetJobFields {
    type Result = OcyResult<job::JobMeta>;
}

impl Handler<GetJobFields> for ApplicationActor {
    type Result = OcyResult<job::JobMeta>;

    fn handle(&mut self, msg: GetJobFields, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).job_fields(msg.0, msg.1.as_ref().map(|v| &**v))
    }
}


/// Message used to ask for the status of given job ID.
pub struct GetJobStatus(pub u64);

impl Message for GetJobStatus {
    type Result = OcyResult<job::Status>;
}

impl Handler<GetJobStatus> for ApplicationActor {
    type Result = OcyResult<job::Status>;

    fn handle(&mut self, msg: GetJobStatus, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).job_status(msg.0)
    }
}


/// Message used to update a job with given information.
pub struct UpdateJob(pub u64, pub job::UpdateRequest);

impl Message for UpdateJob {
    type Result = OcyResult<()>;
}

impl Handler<UpdateJob> for ApplicationActor {
    type Result = OcyResult<()>;

    fn handle(&mut self, msg: UpdateJob, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).update_job(msg.0, &msg.1)
    }
}


/// Message used to send a heartbeat for given job ID.
pub struct Heartbeat(pub u64);

impl Message for Heartbeat {
    type Result = OcyResult<()>;
}

impl Handler<Heartbeat> for ApplicationActor {
    type Result = OcyResult<()>;

    fn handle(&mut self, msg: Heartbeat, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).update_job_heartbeat(msg.0)
    }
}


/// Message used to delete a job with given ID.
pub struct DeleteJob(pub u64);

impl Message for DeleteJob {
    type Result = OcyResult<bool>;
}

impl Handler<DeleteJob> for ApplicationActor {
    type Result = OcyResult<bool>;

    fn handle(&mut self, msg: DeleteJob, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).delete_job(msg.0)
    }
}


/// Message used to get the output field of a single job.
pub struct GetJobOutput(pub u64);

impl Message for GetJobOutput {
    type Result = OcyResult<serde_json::Value>;
}

impl Handler<GetJobOutput> for ApplicationActor {
    type Result = OcyResult<serde_json::Value>;

    fn handle(&mut self, msg: GetJobOutput, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).job_output(msg.0)
    }
}


/// Message used to set the output field of a single job.
pub struct SetJobOutput(pub u64, pub serde_json::Value);

impl Message for SetJobOutput {
    type Result = OcyResult<()>;
}

impl Handler<SetJobOutput> for ApplicationActor {
    type Result = OcyResult<()>;

    fn handle(&mut self, msg: SetJobOutput, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).set_job_output(msg.0, &msg.1)
    }
}


/// Message used to check queue system for any running jobs that should be timed out.
pub struct CheckJobTimeouts;

impl Message for CheckJobTimeouts {
    type Result = OcyResult<Vec<u64>>;
}

impl Handler<CheckJobTimeouts> for ApplicationActor {
    type Result = OcyResult<Vec<u64>>;

    fn handle(&mut self, _msg: CheckJobTimeouts, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).check_job_timeouts()
    }
}


/// Message used to check queue system for any ended jobs that should be removed from the system.
pub struct CheckJobExpiry;

impl Message for CheckJobExpiry {
    type Result = OcyResult<Vec<u64>>;
}

impl Handler<CheckJobExpiry> for ApplicationActor {
    type Result = OcyResult<Vec<u64>>;

    fn handle(&mut self, _msg: CheckJobExpiry, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).check_job_expiry()
    }
}


/// Message used to check queue system for any failed jobs that should be retried.
pub struct CheckJobRetries;

impl Message for CheckJobRetries {
    type Result = OcyResult<Vec<u64>>;
}

impl Handler<CheckJobRetries> for ApplicationActor {
    type Result = OcyResult<Vec<u64>>;

    fn handle(&mut self, _msg: CheckJobRetries, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).check_job_retries()
    }
}


/// Message used to check DB integrity (e.g. that data in index is valid, job state is valid etc.).
///
/// Primarily to be used during development, has a runtime penalty, so shouldn't be run in production.
pub struct CheckDbIntegrity;

impl Message for CheckDbIntegrity {
    type Result = OcyResult<()>;
}

impl Handler<CheckDbIntegrity> for ApplicationActor {
    type Result = OcyResult<()>;

    fn handle(&mut self, _msg: CheckDbIntegrity, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).check_db_integrity()
    }
}


/// Message used to check basic health of system. Checks basic Redis connectivity and response.
pub struct CheckHealth;

impl Message for CheckHealth {
    type Result = OcyResult<()>;
}

impl Handler<CheckHealth> for ApplicationActor {
    type Result = OcyResult<()>;

    fn handle(&mut self, _msg: CheckHealth, _ctx: &mut Self::Context) -> Self::Result {
        RedisManager::new(self.get_connection()?).check_ping()
    }
}
