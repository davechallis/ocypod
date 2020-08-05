//! Integration tests.
//!
//! Requires Redis to be installed, so that the tests can start/stop Redis servers as necessary
//! using the `redis-server` binary.

use std::time;
use std::collections::HashMap;
use redis::aio::Connection;
use ocypod::application::RedisManager;
use ocypod::models::{queue, job, ServerInfo, Duration, OcyError};
use crate::support::*;

mod support;

const DEFAULT_QUEUE: &str = "default";

/// Starts a new `redis-server`, and opens a connection to it. Generally used at the start of every
/// test to ensure that a clean Redis database is used.
async fn init() -> (TestContext, Connection) {
    let ctx = TestContext::new();
    let conn = ctx.async_connection().await.unwrap();
    (ctx, conn)
}

/// Test helper, provides convenient functions for operating on queues for tests.
struct QueueWrapper {
    queue_name: String,
}

impl QueueWrapper {
    fn new<S: Into<String>>(queue_name: S) -> Self {
        Self { queue_name: queue_name.into() }
    }

    async fn with_default_queue(conn: &mut Connection) -> Self {
        let qw = Self::new(DEFAULT_QUEUE);
        qw.create_queue(conn).await;
        qw
    }

    async fn queue_size(&self, conn: &mut Connection) -> u64 {
        RedisManager::queue_size(conn, &self.queue_name).await.unwrap()
    }

    async fn create_queue(&self, conn: &mut Connection) {
        assert_eq!(
            RedisManager::queue_size(conn, &self.queue_name).await,
            Err(OcyError::NoSuchQueue(self.queue_name.clone()))
        );
        assert!(RedisManager::create_or_update_queue(
            conn, &self.queue_name, &queue::Settings::default()
        ).await.unwrap());
    }

    async fn new_job(&self, conn: &mut Connection, job_req: &job::CreateRequest) -> job::JobMeta {
        let job_id = RedisManager::create_job(conn, &self.queue_name, job_req).await.unwrap();
        let job_info = self.job_meta(conn, job_id).await;
        assert_eq!(job_info.status(), job::Status::Queued);
        job_info
    }

    async fn new_default_job(&self, conn: &mut Connection) -> job::JobMeta {
        self.new_job(conn, &job::CreateRequest::default()).await
    }

    async fn new_running_job(&self, conn: &mut Connection, job_req: &job::CreateRequest) -> job::Payload {
        self.new_job(conn, job_req).await;
        self.next_job(conn).await
    }

    async fn new_running_default_job(&self, conn: &mut Connection) -> job::Payload {
        self.new_running_job(conn, &job::CreateRequest::default()).await
    }

    async fn job_fields(&self, conn: &mut Connection, job_id: u64, fields: &[job::Field]) -> job::JobMeta {
        RedisManager::job_fields(conn, job_id, Some(fields)).await.unwrap()
    }

    async fn fail_job(&self, conn: &mut Connection, job_id: u64) -> job::JobMeta {
        let update_req = job::UpdateRequest { status: Some(job::Status::Failed), output: None };
        RedisManager::update_job(conn, job_id, &update_req).await.unwrap();
        let job_info = self.job_meta(conn, job_id).await;
        assert_eq!(job_info.status(), job::Status::Failed);
        job_info
    }

    async fn complete_job(&self, conn: &mut Connection, job_id: u64) -> job::JobMeta {
        let update_req = job::UpdateRequest { status: Some(job::Status::Completed), output: None };
        RedisManager::update_job(conn, job_id, &update_req).await.unwrap();
        let job_info = self.job_meta(conn, job_id).await;
        assert_eq!(job_info.status(), job::Status::Completed);
        assert!(job_info.started_at().is_some());
        assert!(job_info.ended_at().is_some());
        job_info
    }

    async fn next_job(&self, conn: &mut Connection) -> job::Payload {
        let job_payload = RedisManager::next_queued_job(conn, &self.queue_name).await.unwrap().unwrap();
        let job_info = self.job_meta(conn, job_payload.id()).await;
        assert_eq!(job_info.status(), job::Status::Running);
        assert!(job_info.started_at().is_some());
        job_payload
    }

    async fn next_empty_job(&self, conn: &mut Connection) {
        assert!(RedisManager::next_queued_job(conn, &self.queue_name).await.unwrap().is_none());
    }

    async fn job_status(&self, conn: &mut Connection, job_id: u64) -> job::Status {
        RedisManager::job_status(conn, job_id).await.unwrap()
    }

    async fn job_meta(&self, conn: &mut Connection, job_id: u64) -> job::JobMeta {
        RedisManager::job_fields(conn, job_id, None).await.unwrap()
    }
}


#[tokio::test]
async fn queue_create_delete() {
    let (_ctx, mut conn) = init().await;

    let queue_settings = queue::Settings::default();
    assert_eq!(RedisManager::delete_queue(&mut conn, DEFAULT_QUEUE).await.unwrap(), false);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, DEFAULT_QUEUE, &queue_settings).await.unwrap(), true);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, DEFAULT_QUEUE, &queue_settings).await.unwrap(), false);
    assert_eq!(RedisManager::delete_queue(&mut conn, DEFAULT_QUEUE).await.unwrap(), true);
}

#[tokio::test]
async fn queue_delete_jobs() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    for _ in 0..10usize {
        qw.new_default_job(&mut conn).await;
    }
    assert_eq!(qw.queue_size(&mut conn).await, 10);

    // create running job
    qw.next_job(&mut conn).await;
    assert_eq!(qw.queue_size(&mut conn).await, 9);

    // create completed job
    let job_payload = qw.next_job(&mut conn).await;
    let update_req = job::UpdateRequest { status: Some(job::Status::Completed), output: None };
    RedisManager::update_job(&mut conn, job_payload.id(), &update_req).await.unwrap();
    assert_eq!(qw.queue_size(&mut conn).await, 8);

    // create failed job
    let job_id = qw.next_job(&mut conn).await.id();
    qw.fail_job(&mut conn, job_id).await;
    assert_eq!(qw.queue_size(&mut conn).await, 7);

    // create cancelled job
    let job_payload = qw.next_job(&mut conn).await;
    let update_req = job::UpdateRequest { status: Some(job::Status::Cancelled), output: None };
    RedisManager::update_job(&mut conn, job_payload.id(), &update_req).await.unwrap();
    assert_eq!(qw.queue_size(&mut conn).await, 6);

    assert_eq!(RedisManager::delete_queue(&mut conn, DEFAULT_QUEUE).await, Ok(true));
    assert_eq!(qw.job_status(&mut conn, 1).await, job::Status::Running);
    assert_eq!(qw.job_status(&mut conn, 2).await, job::Status::Completed);
    assert_eq!(qw.job_status(&mut conn, 3).await, job::Status::Failed);
    assert_eq!(qw.job_status(&mut conn, 4).await, job::Status::Cancelled);

    // queued jobs should have been deleted
    for job_id in 5..10 {
        assert_eq!(RedisManager::job_status(&mut conn, job_id).await, Err(OcyError::NoSuchJob(job_id)));
    }
}

#[tokio::test]
async fn queue_names() {
    let (_ctx, mut conn) = init().await;

    let queue_settings = queue::Settings::default();
    assert_eq!(RedisManager::queue_names(&mut conn).await.unwrap(), Vec::<String>::new());
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, "d", &queue_settings).await.unwrap(), true);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, "b", &queue_settings).await.unwrap(), true);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, "a", &queue_settings).await.unwrap(), true);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, "c", &queue_settings).await.unwrap(), true);
    assert_eq!(RedisManager::queue_names(&mut conn).await.unwrap(), vec!["a", "b", "c", "d"]);
}

#[tokio::test]
async fn queue_settings() {
    let (_ctx, mut conn) = init().await;
    let queue_name = "a";
    let qw = QueueWrapper::new(queue_name);
    let mut settings = queue::Settings {
        timeout: Duration::from_secs(600),
        heartbeat_timeout: Duration::from_secs(30),
        expires_after: Duration::from_secs(86400),
        retries: 0,
        retry_delays: Vec::new(),
    };
    assert_eq!(RedisManager::queue_settings(&mut conn, queue_name).await, Err(OcyError::NoSuchQueue(queue_name.to_owned())));
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, queue_name, &settings).await, Ok(true));
    assert_eq!(RedisManager::queue_settings(&mut conn, queue_name).await.unwrap(), settings);

    settings.timeout = Duration::from_secs(0);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, queue_name, &settings).await, Ok(false));
    assert_eq!(RedisManager::queue_settings(&mut conn, queue_name).await.unwrap(), settings);

    settings.heartbeat_timeout = Duration::from_secs(1_000_000);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, queue_name, &settings).await, Ok(false));
    assert_eq!(RedisManager::queue_settings(&mut conn, queue_name).await.unwrap(), settings);

    settings.expires_after = Duration::from_secs(1234);
    assert_eq!(RedisManager::create_or_update_queue(&mut conn, queue_name, &settings).await, Ok(false));
    assert_eq!(RedisManager::queue_settings(&mut conn, queue_name).await.unwrap(), settings);
}

#[tokio::test]
async fn queue_size() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    assert_eq!(qw.queue_size(&mut conn).await, 0);

    for _ in 0..3usize {
        qw.new_default_job(&mut conn).await;
    }
    assert_eq!(qw.queue_size(&mut conn).await, 3);

    for _ in 0..2usize {
        qw.next_job(&mut conn).await;
    }
    assert_eq!(qw.queue_size(&mut conn).await, 1);

    for _ in 0..4usize {
        qw.new_default_job(&mut conn).await;
    }
    assert_eq!(qw.queue_size(&mut conn).await, 5);

    // drain queue
    for _ in 0..5usize {
        qw.next_job(&mut conn).await;
    }

    // keep requesting more jobs, ensure queue size doesn't go below 0
    for _ in 0..5usize {
        qw.next_empty_job(&mut conn).await;
    }
    assert_eq!(qw.queue_size(&mut conn).await, 0);
}

#[tokio::test]
async fn basic_summary() {
    let (_ctx, mut conn) = init().await;
    let expected = ServerInfo::default();
    let summary = RedisManager::server_info(&mut conn).await.unwrap();
    assert_eq!(summary, expected);
}

#[tokio::test]
async fn job_not_exists() {
    let (_ctx, mut conn) = init().await;
    assert_eq!(RedisManager::job_fields(&mut conn, 1, None).await, Err(OcyError::NoSuchJob(1)));
    assert_eq!(RedisManager::job_status(&mut conn, 1).await, Err(OcyError::NoSuchJob(1)));
}

#[tokio::test]
async fn job_creation() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    let job_id = qw.new_default_job(&mut conn).await.id();
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.id(), 1);
    assert_eq!(job_info.id(), job_id);
    assert_eq!(job_info.queue(), DEFAULT_QUEUE);
    assert_eq!(job_info.status(), job::Status::Queued);
    assert!(job_info.started_at().is_none());
    assert!(job_info.ended_at().is_none());
    assert!(job_info.last_heartbeat().is_none());
    assert!(job_info.input().is_none());
    assert!(job_info.output().is_none());
    // TODO: check job timeouts match queue timeouts
    // TODO: check job retries match queue retries

    let job_req = job::CreateRequest { input: Some("string".into()), ..Default::default() };
    assert_eq!(qw.new_job(&mut conn, &job_req).await.id(), 2);
    let job_info = qw.job_meta(&mut conn, 2).await;
    assert_eq!(job_info.id(), 2);
    assert_eq!(job_info.input(), job_req.input);
}

#[tokio::test]
async fn job_output() {
    let (_ctx, mut conn) = init().await;
    assert_eq!(RedisManager::job_output(&mut conn, 123).await, Err(OcyError::NoSuchJob(123)));
    // TODO: populate test
}

#[tokio::test]
async fn job_deletion() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    let tags = vec!["tag".to_string()];
    let job_req = job::CreateRequest { tags: Some(tags), ..Default::default() };

    let jobs = create_job_in_all_states(&mut conn, DEFAULT_QUEUE, &job_req).await;
    let job_id_running = jobs[&job::Status::Running];
    let job_id_completed = jobs[&job::Status::Completed];
    let job_id_failed = jobs[&job::Status::Failed];
    let job_id_cancelled = jobs[&job::Status::Cancelled];
    let job_id_timed_out = jobs[&job::Status::TimedOut];
    let job_id_queued = jobs[&job::Status::Queued];

    assert_eq!(RedisManager::running_queue_size(&mut conn).await.unwrap(), 1);
    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_running));
    assert!(RedisManager::delete_job(&mut conn, job_id_running).await.unwrap());
    assert_eq!(RedisManager::running_queue_size(&mut conn, ).await.unwrap(), 0);
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_running));

    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_completed));
    assert!(RedisManager::delete_job(&mut conn, job_id_completed).await.unwrap());
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_completed));

    assert_eq!(RedisManager::failed_queue_size(&mut conn, ).await.unwrap(), 2);
    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_failed));
    assert!(RedisManager::delete_job(&mut conn, job_id_failed).await.unwrap());
    assert_eq!(RedisManager::failed_queue_size(&mut conn).await.unwrap(), 1);
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_failed));

    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_cancelled));
    assert!(RedisManager::delete_job(&mut conn, job_id_cancelled).await.unwrap());
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_cancelled));

    assert_eq!(RedisManager::failed_queue_size(&mut conn).await.unwrap(), 1);
    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_timed_out));
    assert!(RedisManager::delete_job(&mut conn, job_id_timed_out).await.unwrap());
    assert_eq!(RedisManager::failed_queue_size(&mut conn).await.unwrap(), 0);
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_timed_out));

    assert_eq!(RedisManager::queue_size(&mut conn, DEFAULT_QUEUE).await.unwrap(), 1);
    assert!(RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_queued));
    assert!(RedisManager::delete_job(&mut conn, job_id_queued).await.unwrap());
    assert_eq!(RedisManager::queue_size(&mut conn, DEFAULT_QUEUE).await.unwrap(), 0);
    assert!(!RedisManager::tagged_job_ids(&mut conn, "tag").await.unwrap().contains(&job_id_queued));

    // ensure deleting non-existing jobs returns false
    assert!(!RedisManager::delete_job(&mut conn, job_id_running).await.unwrap());
    assert!(!RedisManager::delete_job(&mut conn, job_id_completed).await.unwrap());
    assert!(!RedisManager::delete_job(&mut conn, job_id_failed).await.unwrap());
    assert!(!RedisManager::delete_job(&mut conn, job_id_cancelled).await.unwrap());
    assert!(!RedisManager::delete_job(&mut conn, job_id_timed_out).await.unwrap());
    assert!(!RedisManager::delete_job(&mut conn, job_id_queued).await.unwrap());
}

#[tokio::test]
async fn job_retry_no_queue() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: Some(Duration::from_secs(0)),
        expires_after: Some(Duration::from_secs(0)),
        retries: Some(3),
        retry_delays: None,
        ..Default::default() };

    // create and start new job running
    let job_id = qw.new_running_job(&mut conn, &job_req).await.id();
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st failure
    let job_info = qw.fail_job(&mut conn, job_id).await;
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // delete job's original queue
    assert!(RedisManager::delete_queue(&mut conn, DEFAULT_QUEUE).await.unwrap());

    // 1st retry
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), Vec::<u64>::new());
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 0);
}

#[tokio::test]
async fn job_retries() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: Some(Duration::from_secs(0)),
        expires_after: Some(Duration::from_secs(0)),
        retries: Some(3),
        retry_delays: None,
        ..Default::default() };

    // create and start new job running
    let job_id = qw.new_running_job(&mut conn, &job_req).await.id();
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st failure
    let job_info = qw.fail_job(&mut conn, job_id).await;
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st retry
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), vec![job_id]);
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 1);

    // ensure it doesn't get retried while already running
    let empty: Vec<u64> = Vec::new();
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), empty);

    // 2nd failure: job timeout
    let job_id = qw.next_job(&mut conn).await.id();
    tokio::time::delay_for(time::Duration::from_secs(2)).await;
    assert_eq!(RedisManager::check_job_timeouts(&mut conn).await.unwrap(), vec![job_id]);
    assert_eq!(qw.job_status(&mut conn, job_id).await, job::Status::TimedOut);

    // 2nd retry
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), vec![job_id]);
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 2);

    // 3rd failure
    let job_id = qw.next_job(&mut conn).await.id();
    qw.fail_job(&mut conn, job_id).await;

    // 3rd retry
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), vec![job_id]);
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 3);

    // fail final time
    let job_id = qw.next_job(&mut conn).await.id();
    qw.fail_job(&mut conn, job_id).await;

    // ensure it doesn't get retries any more
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), empty);
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 3);
}

#[tokio::test]
async fn job_retry_delays() {
    let (_ctx, mut conn) = init().await;
    let qw = QueueWrapper::with_default_queue(&mut conn).await;

    let delays = vec![Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)];
    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(0)),
        heartbeat_timeout: Some(Duration::from_secs(0)),
        expires_after: Some(Duration::from_secs(0)),
        retries: Some(3),
        retry_delays: Some(delays.clone()),
        ..Default::default() };

    let empty: Vec<u64> = Vec::new();

    // create and start new job running
    let job_id = qw.new_running_job(&mut conn, &job_req).await.id();

    // 1st failure
    qw.fail_job(&mut conn, job_id).await;
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), Some(delays));

    // no retry yet
    assert_eq!(RedisManager::check_job_retries(&mut conn).await.unwrap(), empty);
    let job_info = qw.job_meta(&mut conn, job_id).await;
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 0);

    // TODO: finish off adding additional retry tests here - i.e. sleep then checking further delay times
}

// #[tokio::test]
// fn tag_creation() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     assert_eq!(qw.new_default_job().tags(), None);

//     // maintain list of all job IDs which have been given tag "foo"
//     let mut foo_tagged = Vec::new();

//     let tags = vec!["foo".to_string(),
//                     "another-tag".to_string(),
//                     "3rd.tag_example".to_string()];
//     let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };
//     let job_id = qw.new_job(&job_req).id();
//     assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
//     foo_tagged.push(job_id);

//     // should get empty list for non-existent tags
//     assert_eq!(RedisManager::tagged_job_ids("some_tag"), Ok(Vec::new()));

//     // should get single job
//     for tag in &tags {
//         assert_eq!(RedisManager::tagged_job_ids(tag), Ok(vec![job_id]));
//     }

//     let tags = vec!["foo".to_string(), "bar".to_string()];
//     let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };

//     let job_id = qw.new_job(&job_req).id();
//     assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
//     foo_tagged.push(job_id);

//     let job_id = qw.new_job(&job_req).id();
//     assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
//     foo_tagged.push(job_id);

//     let job_id = qw.new_job(&job_req).id();
//     assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
//     foo_tagged.push(job_id);

//     foo_tagged.sort();
//     assert_eq!(RedisManager::tagged_job_ids("foo"), Ok(foo_tagged));
// }

// #[tokio::test]
// fn tag_deletion() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let tags = vec!["a".to_string(), "b".to_string(), "c".to_string()];

//     // ensure no job IDs are returned for non-existent tags
//     for tag in &tags {
//         assert_eq!(RedisManager::tagged_job_ids(tag), Ok(Vec::new()));
//     }

//     // ensure tag info returned for jobs, and job info returned for tags
//     let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };
//     let tagged_ids: Vec<u64> = (0..4).map(|_| {
//         let job_id = qw.new_job(&job_req).id();
//         assert_eq!(RedisManager::job_fields(job_id, None).unwrap().tags(), Some(tags.clone()));
//         job_id
//     }).collect();

//     for tag in &tags {
//         assert_eq!(RedisManager::tagged_job_ids(tag).unwrap(), tagged_ids);
//     }

//     // TODO: add when deletion actually added
// }

// #[tokio::test]
// fn job_starting() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let input: serde_json::Value = vec![1, 2, 3].into();
//     let mut job_req = job::CreateRequest::default();
//     job_req.input = Some(input.clone());
//     let job_id = qw.new_job(&job_req).id();

//     let job_payload = qw.next_job();
//     assert_eq!(job_payload.id(), job_id);
//     assert_eq!(job_payload.input(), &Some(input.clone()));

//     let job_info = qw.job_meta(job_id);
//     assert_eq!(job_info.queue(), DEFAULT_QUEUE);
//     assert_eq!(job_info.status(), job::Status::Running);
//     assert!(job_info.started_at().is_some());
//     assert!(job_info.ended_at().is_none());
//     assert!(job_info.last_heartbeat().is_none());
//     assert_eq!(job_info.input(), Some(input));
//     assert!(job_info.output().is_none());
// }

// #[tokio::test]
// fn job_fields() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let job_meta = qw.new_default_job();
//     let job_id = job_meta.id();

//     // single fields
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Id]).id(), 1);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Queue]).queue(), DEFAULT_QUEUE);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Status]).status(), job::Status::Queued);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Tags]).tags(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::CreatedAt]).created_at(), job_meta.created_at());
//     assert_eq!(qw.job_fields(job_id, &[job::Field::StartedAt]).started_at(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::EndedAt]).ended_at(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::LastHeartbeat]).last_heartbeat(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Input]).input(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Output]).output(), None);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Timeout]).timeout(), job_meta.timeout());
//     assert_eq!(qw.job_fields(job_id, &[job::Field::HeartbeatTimeout]).heartbeat_timeout(), job_meta.heartbeat_timeout());
//     assert_eq!(qw.job_fields(job_id, &[job::Field::ExpiresAfter]).expires_after(), job_meta.expires_after());
//     assert_eq!(qw.job_fields(job_id, &[job::Field::Retries]).retries(), job_meta.retries());
//     assert_eq!(qw.job_fields(job_id, &[job::Field::RetriesAttempted]).retries_attempted(), 0);
//     assert_eq!(qw.job_fields(job_id, &[job::Field::RetryDelays]).retry_delays(), None);

//     // multiple fields
//     let jm = qw.job_fields(job_id, &[job::Field::Status, job::Field::Output]);
//     assert_eq!(jm.status(), job_meta.status());
//     assert_eq!(jm.output(), job_meta.output());

//     let jm = qw.job_fields(job_id, &[job::Field::Id, job::Field::Tags, job::Field::RetryDelays]);
//     assert_eq!(jm.id(), job_meta.id());
//     assert_eq!(jm.tags(), job_meta.tags());
//     assert_eq!(jm.retry_delays(), job_meta.retry_delays());

//     // all fields
//     let jm = RedisManager::job_fields(job_id, None).unwrap();
//     assert_eq!(jm.id(), job_meta.id());
//     assert_eq!(jm.status(), job_meta.status());
//     assert_eq!(jm.output(), job_meta.output());
//     assert_eq!(jm.tags(), job_meta.tags());
//     assert_eq!(jm.retry_delays(), job_meta.retry_delays());

// }

// #[tokio::test]
// fn update_job_heartbeat() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     qw.new_default_job();
//     let job_id = qw.next_job().id();
//     assert!(qw.job_meta(job_id).last_heartbeat().is_none());

//     RedisManager::update_job_heartbeat(job_id).unwrap();
//     let hb1 = qw.job_meta(job_id).last_heartbeat().unwrap();

//     sleep(time::Duration::from_secs(1));

//     RedisManager::update_job_heartbeat(job_id).unwrap();
//     let hb2 = qw.job_meta(job_id).last_heartbeat().unwrap();

//     assert!(hb2 > hb1);
// }

// #[tokio::test]
// fn update_job_output() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     // can only update jobs if they exist
//     match RedisManager::set_job_output(21, &"foo".into()) {
//         Err(OcyError::NoSuchJob(21)) => (),
//         x                              => assert!(false, "Unexpected result: {:?}", x),
//     }

//     let job_id = qw.new_default_job().id();

//     // can only update output for running jobs
//     match RedisManager::set_job_output(job_id, &"foo".into()) {
//         Err(OcyError::Conflict(_)) => (),
//         x                              => assert!(false, "Unexpected result: {:?}", x),
//     }

//     // ensure output is initially unset
//     let job_id = qw.next_job().id();
//     assert!(qw.job_meta(job_id).output().is_none());

//     // ensure output is set
//     RedisManager::set_job_output(job_id, &"foo".into()).unwrap();
//     assert_eq!(qw.job_meta(job_id).output(), Some("foo".into()));

//     // ensure output is overwritten
//     let map = serde_json::from_str("{\"a\": 1, \"b\": 2}").unwrap();
//     RedisManager::set_job_output(job_id, &map).unwrap();
//     assert_eq!(qw.job_meta(job_id).output(), Some(map));
// }

// #[tokio::test]
// fn queued_status_transitions() {
//     // TODO: add transitions for retries:
//     // Cancelled -> Queued
//     // Failed -> Queued
//     // TimedOut -> Queued
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     // cannot manually change from Queued, only `next_job` does this
//     let job_id = qw.new_default_job().id();
//     let not_allowed = &[job::Status::Queued,
//                         job::Status::Running,
//                         job::Status::Failed,
//                         job::Status::Completed,
//                         job::Status::TimedOut];
//     for new_status in not_allowed {
//         match RedisManager::set_job_status(job_id, new_status) {
//             Err(OcyError::Conflict(_)) => (),
//             x => assert!(false, "Unexpected result when changing status Queued -> {}: {:?}", new_status, x),
//         }
//     }

//     // queued jobs can be cancelled
//     RedisManager::set_job_status(job_id, &job::Status::Cancelled).unwrap();
//     let job_info = qw.job_meta(job_id);
//     assert_eq!(job_info.status(), job::Status::Cancelled);
//     assert!(job_info.ended_at().is_some());
// }

// #[tokio::test]
// fn running_status_transitions() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let not_allowed = &[
//         job::Status::Queued,   // cannot re-queue a running job
//         job::Status::Running,  // job is already running
//     ];
//     let job_id = qw.new_running_default_job().id();
//     for new_status in not_allowed {
//         match RedisManager::set_job_status(job_id, new_status) {
//             Err(OcyError::Conflict(_)) => (),
//             x => assert!(false, "Unexpected result when changing status Running -> {}: {:?}", new_status, x),
//         }
//     }

//     let allowed = &[
//         job::Status::Cancelled,
//         job::Status::Completed,
//         job::Status::Failed,
//         job::Status::TimedOut, // TODO: should this be limited to being set by the server only?
//     ];
//     for new_status in allowed {
//         let job_id = qw.new_running_default_job().id();
//         RedisManager::set_job_status(job_id, new_status).unwrap();
//         let job_info = qw.job_meta(job_id);
//         assert_eq!(job_info.status(), *new_status);
//         assert!(job_info.ended_at().is_some());
//     }
// }

// #[tokio::test]
// fn completed_status_transitions() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     // once completed, job status cannot be changed
//     let job_id = qw.new_running_default_job().id();
//     qw.complete_job(job_id);
//     let not_allowed = &[
//         job::Status::Completed,
//         job::Status::Queued,
//         job::Status::Running,
//         job::Status::Failed,
//         job::Status::Completed,
//         job::Status::TimedOut
//     ];
//     for new_status in not_allowed {
//         match RedisManager::set_job_status(job_id, new_status) {
//             Err(OcyError::Conflict(_)) => (),
//             x => assert!(false, "Unexpected result when changing status Completed -> {}: {:?}", new_status, x),
//         }
//     }
// }

// #[tokio::test]
// fn check_ping() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);
//     RedisManager::check_ping().unwrap();
// }

// #[tokio::test]
// fn job_heartbeat_timeout() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let mut job_req = job::CreateRequest {
//         timeout: Some(Duration::from_secs(3600)),
//         heartbeat_timeout: Some(Duration::from_secs(1)),
//         expires_after: Some(Duration::from_secs(3600)),
//         ..Default::default()
//     };
//     let job_id_a = qw.new_job(&job_req).id();

//     job_req.heartbeat_timeout = Some(Duration::from_secs(3));
//     let job_id_b = qw.new_job(&job_req).id();
//     let job_id_c = qw.new_job(&job_req).id();

//     qw.next_job();
//     qw.next_job();
//     qw.next_job();

//     assert_eq!(qw.job_status(job_id_a), job::Status::Running);
//     assert_eq!(qw.job_status(job_id_b), job::Status::Running);
//     assert_eq!(qw.job_status(job_id_c), job::Status::Running);

//     sleep(time::Duration::from_secs(2));
//     assert_eq!(RedisManager::check_job_timeouts().unwrap(), vec![job_id_a]);
//     assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
//     assert_eq!(qw.job_status(job_id_b), job::Status::Running);
//     assert_eq!(qw.job_status(job_id_c), job::Status::Running);

//     RedisManager::update_job_heartbeat(job_id_c).unwrap();
//     sleep(time::Duration::from_secs(2));
//     assert_eq!(RedisManager::check_job_timeouts().unwrap(), vec![job_id_b]);
//     assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
//     assert_eq!(qw.job_status(job_id_b), job::Status::TimedOut);
//     assert_eq!(qw.job_status(job_id_c), job::Status::Running);

//     assert_eq!(qw.job_meta(job_id_a).last_heartbeat(), None);
//     assert_eq!(qw.job_meta(job_id_b).last_heartbeat(), None);
//     assert!(qw.job_meta(job_id_c).last_heartbeat().is_some());
// }

// #[tokio::test]
// fn job_timeout() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let mut job_req = job::CreateRequest {
//         timeout: Some(Duration::from_secs(1)),
//         heartbeat_timeout: Some(Duration::from_secs(3600)),
//         expires_after: Some(Duration::from_secs(3600)),
//         ..Default::default()
//     };
//     let job_id_a = qw.new_job(&job_req).id();

//     job_req.timeout = Some(Duration::from_secs(3));
//     let job_id_b = qw.new_job(&job_req).id();

//     qw.next_job();
//     qw.next_job();

//     assert_eq!(qw.job_status(job_id_a), job::Status::Running);
//     assert_eq!(qw.job_status(job_id_b), job::Status::Running);

//     sleep(time::Duration::from_secs(2));
//     assert_eq!(RedisManager::check_job_timeouts().unwrap(), vec![1]);
//     assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
//     assert_eq!(qw.job_status(job_id_b), job::Status::Running);
// }

// #[tokio::test]
// fn job_expiry() {
//     let (_ctx, mut conn) = init().await;
//     let qw = QueueWrapper::with_default_queue(&conn);

//     let job_req = job::CreateRequest {
//         timeout: Some(Duration::from_secs(3600)),
//         heartbeat_timeout: Some(Duration::from_secs(3600)),
//         expires_after: Some(Duration::from_secs(1)),
//         retries: Some(0),
//         ..Default::default()
//     };

//     let jobs = create_job_in_all_states(&qw.manager, DEFAULT_QUEUE, &job_req);
//     let job_id_running = jobs[&job::Status::Running];
//     let job_id_completed = jobs[&job::Status::Completed];
//     let job_id_failed = jobs[&job::Status::Failed];
//     let job_id_cancelled = jobs[&job::Status::Cancelled];
//     let job_id_timed_out = jobs[&job::Status::TimedOut];
//     let job_id_queued = jobs[&job::Status::Queued];

//     // confirm all jobs are in correct queues
//     assert_eq!(RedisManager::running_queue_size().unwrap(), 1);
//     assert_eq!(RedisManager::failed_queue_size().unwrap(), 2); // one failed, one timed out
//     assert_eq!(RedisManager::ended_queue_size().unwrap(), 2); // one completed, one cancelled
//     assert_eq!(qw.queue_size(), 1);

//     // jobs in ended queue (i.e. one completed and one cancelled) should expire
//     let mut expected_expired = vec![job_id_completed, job_id_cancelled,];
//     expected_expired.sort();
//     sleep(time::Duration::from_secs(2));
//     let mut expired = RedisManager::check_job_expiry().unwrap();
//     expired.sort();
//     assert_eq!(expired, expected_expired);

//     // no retries, failed/timed out jobs should be moved to ended queue, then expire next call
//     assert_eq!(RedisManager::check_job_retries().unwrap(), Vec::<u64>::new());
//      let mut expected_expired = vec![job_id_failed, job_id_timed_out];
//     expected_expired.sort();
//     sleep(time::Duration::from_secs(2));
//     let mut expired = RedisManager::check_job_expiry().unwrap();
//     expired.sort();
//     assert_eq!(expired, expected_expired);

//     assert_eq!(RedisManager::job_status(job_id_running),   Ok(job::Status::Running));
//     assert_eq!(RedisManager::job_status(job_id_completed), Err(OcyError::NoSuchJob(job_id_completed)));
//     assert_eq!(RedisManager::job_status(job_id_failed),    Err(OcyError::NoSuchJob(job_id_failed)));
//     assert_eq!(RedisManager::job_status(job_id_cancelled), Err(OcyError::NoSuchJob(job_id_cancelled)));
//     assert_eq!(RedisManager::job_status(job_id_timed_out), Err(OcyError::NoSuchJob(job_id_timed_out)));
//     assert_eq!(RedisManager::job_status(job_id_queued),    Ok(job::Status::Queued));
// }

async fn create_job_in_all_states(
    conn: &mut Connection,
    queue: &str,
    create_req: &job::CreateRequest
) -> HashMap<job::Status, u64> {
     let mut job_req: job::CreateRequest = create_req.clone();

    let job_id_running = RedisManager::create_job(conn, queue, &job_req).await.unwrap();
    let job_id_completed = RedisManager::create_job(conn, queue, &job_req).await.unwrap();
    let job_id_failed = RedisManager::create_job(conn, queue, &job_req).await.unwrap();
    let job_id_cancelled = RedisManager::create_job(conn, queue, &job_req).await.unwrap();

    job_req.timeout = Some(Duration::from_secs(1));
    let job_id_timed_out = RedisManager::create_job(conn, queue, &job_req).await.unwrap();
    let job_id_queued = RedisManager::create_job(conn, queue, &job_req).await.unwrap();

    let mut update_req = job::UpdateRequest::default();

    // leave 1st job as running
    RedisManager::next_queued_job(conn, queue).await.unwrap();

    // complete 2nd job
    RedisManager::next_queued_job(conn, queue).await.unwrap();
    update_req.status = Some(job::Status::Completed);
    RedisManager::update_job(conn, job_id_completed, &update_req).await.unwrap();

    // fail 3rd job
    RedisManager::next_queued_job(conn, queue).await.unwrap();
    update_req.status = Some(job::Status::Failed);
    RedisManager::update_job(conn, job_id_failed, &update_req).await.unwrap();

    // cancel 4th job
    RedisManager::next_queued_job(conn, queue).await.unwrap();
    update_req.status = Some(job::Status::Cancelled);
    RedisManager::update_job(conn, job_id_cancelled, &update_req).await.unwrap();

    // 5th job should timeout
    RedisManager::next_queued_job(conn, queue).await.unwrap();

    tokio::time::delay_for(time::Duration::from_secs(2)).await;
    assert_eq!(RedisManager::check_job_timeouts(conn).await.unwrap(), vec![job_id_timed_out]);

    assert_eq!(RedisManager::job_status(conn, job_id_running).await, Ok(job::Status::Running));
    assert_eq!(RedisManager::job_status(conn, job_id_completed).await, Ok(job::Status::Completed));
    assert_eq!(RedisManager::job_status(conn, job_id_failed).await, Ok(job::Status::Failed));
    assert_eq!(RedisManager::job_status(conn, job_id_cancelled).await, Ok(job::Status::Cancelled));
    assert_eq!(RedisManager::job_status(conn, job_id_timed_out).await, Ok(job::Status::TimedOut));
    assert_eq!(RedisManager::job_status(conn, job_id_queued).await, Ok(job::Status::Queued));

    let mut map = HashMap::with_capacity(6);
    map.insert(job::Status::Running, job_id_running);
    map.insert(job::Status::Completed, job_id_completed);
    map.insert(job::Status::Failed, job_id_failed);
    map.insert(job::Status::Cancelled, job_id_cancelled);
    map.insert(job::Status::TimedOut, job_id_timed_out);
    map.insert(job::Status::Queued, job_id_queued);
    map
}
