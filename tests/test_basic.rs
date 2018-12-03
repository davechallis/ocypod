//! Integration tests.
//!
//! Requires Redis to be installed, so that the tests can start/stop Redis servers as necessary
//! using the `redis-server` binary.

extern crate ocypod;
extern crate redis;
extern crate serde_json;

use std::thread::sleep;
use std::time;
use std::collections::HashMap;
use ocypod::application::RedisManager;
use ocypod::models::{queue, job, ServerInfo, Duration, OcyError};
use crate::support::*;

mod support;

const DEFAULT_QUEUE: &str = "default";

/// Starts a new `redis-server`, and opens a connection to it. Generally used at the start of every
/// test to ensure that a clean Redis database is used.
fn init() -> (TestContext, redis::Connection) {
    let ctx = TestContext::new();
    let conn = ctx.connection();
    (ctx, conn)
}

/// Test helper, provides convenient functions for operating on queues for tests.
struct QueueWrapper<'a> {
    manager: RedisManager<'a>,
    queue_name: String,
}

impl<'a> QueueWrapper<'a> {
    fn new(conn: &'a redis::Connection, queue_name: &str) -> Self {
        Self { manager: RedisManager::new(conn), queue_name: queue_name.to_owned() }
    }

    fn with_default_queue(conn: &'a redis::Connection) -> Self {
        let qw = Self::new(conn, DEFAULT_QUEUE);
        qw.create_queue();
        qw
    }

    fn queue_size(&self) -> u64 {
        self.manager.queue_size(&self.queue_name).unwrap()
    }

    fn create_queue(&self) {
        assert_eq!(self.manager.queue_size(&self.queue_name), Err(OcyError::NoSuchQueue(self.queue_name.clone())));
        assert!(self.manager.create_or_update_queue(&self.queue_name, &queue::Settings::default()).unwrap());
    }

    fn new_job(&self, job_req: &job::CreateRequest) -> job::JobMeta {
        let job_id = self.manager.create_job(&self.queue_name, job_req).unwrap();
        let job_info = self.job_meta(job_id);
        assert_eq!(job_info.status(), job::Status::Queued);
        job_info
    }

    fn new_default_job(&self) -> job::JobMeta {
        self.new_job(&job::CreateRequest::default())
    }

    fn new_running_job(&self, job_req: &job::CreateRequest) -> job::Payload {
        self.new_job(job_req);
        self.next_job()
    }

    fn new_running_default_job(&self) -> job::Payload {
        self.new_running_job(&job::CreateRequest::default())
    }

    fn job_fields(&self, job_id: u64, fields: &[job::Field]) -> job::JobMeta {
        self.manager.job_fields(job_id, Some(fields)).unwrap()
    }

    fn fail_job(&self, job_id: u64) -> job::JobMeta {
        let update_req = job::UpdateRequest { status: Some(job::Status::Failed), output: None };
        self.manager.update_job(job_id, &update_req).unwrap();
        let job_info = self.job_meta(job_id);
        assert_eq!(job_info.status(), job::Status::Failed);
        job_info
    }

    fn complete_job(&self, job_id: u64) -> job::JobMeta {
        let update_req = job::UpdateRequest { status: Some(job::Status::Completed), output: None };
        self.manager.update_job(job_id, &update_req).unwrap();
        let job_info = self.job_meta(job_id);
        assert_eq!(job_info.status(), job::Status::Completed);
        assert!(job_info.started_at().is_some());
        assert!(job_info.ended_at().is_some());
        job_info
    }

    fn next_job(&self) -> job::Payload {
        let job_payload = self.manager.next_queued_job(&self.queue_name).unwrap().unwrap();
        let job_info = self.job_meta(job_payload.id());
        assert_eq!(job_info.status(), job::Status::Running);
        assert!(job_info.started_at().is_some());
        job_payload
    }

    fn next_empty_job(&self) {
        assert!(self.manager.next_queued_job(&self.queue_name).unwrap().is_none());
    }

    fn job_status(&self, job_id: u64) -> job::Status {
        self.manager.job_status(job_id).unwrap()
    }

    fn job_meta(&self, job_id: u64) -> job::JobMeta {
        self.manager.job_fields(job_id, None).unwrap()
    }
}


#[test]
fn queue_create_delete() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::new(&conn, DEFAULT_QUEUE);
    let queue_settings = queue::Settings::default();
    assert_eq!(qw.manager.delete_queue(DEFAULT_QUEUE).unwrap(), false);
    assert_eq!(qw.manager.create_or_update_queue(DEFAULT_QUEUE, &queue_settings).unwrap(), true);
    assert_eq!(qw.manager.create_or_update_queue(DEFAULT_QUEUE, &queue_settings).unwrap(), false);
    assert_eq!(qw.manager.delete_queue(DEFAULT_QUEUE).unwrap(), true);
}

#[test]
fn queue_delete_jobs() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    for _ in 0..10 {
        qw.new_default_job();
    }
    assert_eq!(qw.queue_size(), 10);

    // create running job
    qw.next_job();
    assert_eq!(qw.queue_size(), 9);

    // create completed job
    let job_payload = qw.next_job();
    let update_req = job::UpdateRequest { status: Some(job::Status::Completed), output: None };
    qw.manager.update_job(job_payload.id(), &update_req).unwrap();
    assert_eq!(qw.queue_size(), 8);

    // create failed job
    let job_id = qw.next_job().id();
    qw.fail_job(job_id);
    assert_eq!(qw.queue_size(), 7);

    // create cancelled job
    let job_payload = qw.next_job();
    let update_req = job::UpdateRequest { status: Some(job::Status::Cancelled), output: None };
    qw.manager.update_job(job_payload.id(), &update_req).unwrap();
    assert_eq!(qw.queue_size(), 6);

    assert_eq!(qw.manager.delete_queue(DEFAULT_QUEUE), Ok(true));
    assert_eq!(qw.job_status(1), job::Status::Running);
    assert_eq!(qw.job_status(2), job::Status::Completed);
    assert_eq!(qw.job_status(3), job::Status::Failed);
    assert_eq!(qw.job_status(4), job::Status::Cancelled);

    // queued jobs should have been deleted
    for job_id in 5..10 {
        assert_eq!(qw.manager.job_status(job_id), Err(OcyError::NoSuchJob(job_id)));
    }
}

#[test]
fn queue_names() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::new(&conn, DEFAULT_QUEUE);

    let queue_settings = queue::Settings::default();
    assert_eq!(qw.manager.queue_names().unwrap(), Vec::<String>::new());
    assert_eq!(qw.manager.create_or_update_queue("d", &queue_settings).unwrap(), true);
    assert_eq!(qw.manager.create_or_update_queue("b", &queue_settings).unwrap(), true);
    assert_eq!(qw.manager.create_or_update_queue("a", &queue_settings).unwrap(), true);
    assert_eq!(qw.manager.create_or_update_queue("c", &queue_settings).unwrap(), true);
    assert_eq!(qw.manager.queue_names().unwrap(), vec!["a", "b", "c", "d"]);
}

#[test]
fn queue_settings() {
    let (_ctx, conn) = init();
    let queue_name = "a";
    let qw = QueueWrapper::new(&conn, queue_name);
    let mut settings = queue::Settings {
        timeout: Duration::from_secs(600),
        heartbeat_timeout: Duration::from_secs(30),
        expires_after: Duration::from_secs(86400),
        retries: 0,
        retry_delays: Vec::new(),
    };
    assert_eq!(qw.manager.queue_settings(queue_name), Err(OcyError::NoSuchQueue(queue_name.to_owned())));
    assert_eq!(qw.manager.create_or_update_queue(queue_name, &settings), Ok(true));
    assert_eq!(qw.manager.queue_settings(queue_name).unwrap(), settings);

    settings.timeout = Duration::from_secs(0);
    assert_eq!(qw.manager.create_or_update_queue(queue_name, &settings), Ok(false));
    assert_eq!(qw.manager.queue_settings(queue_name).unwrap(), settings);

    settings.heartbeat_timeout = Duration::from_secs(1_000_000);
    assert_eq!(qw.manager.create_or_update_queue(queue_name, &settings), Ok(false));
    assert_eq!(qw.manager.queue_settings(queue_name).unwrap(), settings);

    settings.expires_after = Duration::from_secs(1234);
    assert_eq!(qw.manager.create_or_update_queue(queue_name, &settings), Ok(false));
    assert_eq!(qw.manager.queue_settings(queue_name).unwrap(), settings);
}

#[test]
fn queue_size() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    assert_eq!(qw.queue_size(), 0);

    for _ in 0..3 {
        qw.new_default_job();
    }
    assert_eq!(qw.queue_size(), 3);

    for _ in 0..2 {
        qw.next_job();
    }
    assert_eq!(qw.queue_size(), 1);

    for _ in 0..4 {
        qw.new_default_job();
    }
    assert_eq!(qw.queue_size(), 5);

    // drain queue
    for _ in 0..5 {
        qw.next_job();
    }

    // keep requesting more jobs, ensure queue size doesn't go below 0
    for _ in 0..5 {
        qw.next_empty_job();
    }
    assert_eq!(qw.queue_size(), 0);
}

#[test]
fn queue_summary_sizes() {
    let (_ctx, conn) = init();
    let queue_name = "a";
    let qw = QueueWrapper::new(&conn, queue_name);

    let queue_settings = queue::Settings {
        timeout: Duration::from_secs(600),
        heartbeat_timeout: Duration::from_secs(30),
        expires_after: Duration::from_secs(86400),
        retries: 0,
        retry_delays: Vec::new(),
    };
    let mut expected_summary = queue::Summary {
        size: 0,
        timeout: Duration::from_secs(600),
        heartbeat_timeout: Duration::from_secs(30),
        expires_after: Duration::from_secs(86400),
    };
    assert_eq!(qw.manager.queue_summary(queue_name), Err(OcyError::NoSuchQueue(queue_name.to_owned())));
    qw.manager.create_or_update_queue(queue_name, &queue_settings).unwrap();
    assert_eq!(&qw.manager.queue_summary(queue_name).unwrap(), &expected_summary);

    qw.new_default_job();
    qw.new_default_job();
    qw.new_default_job();
    qw.new_default_job();
    expected_summary.size = 4;
    assert_eq!(&qw.manager.queue_summary(queue_name).unwrap(), &expected_summary);

    qw.next_job();
    qw.next_job();
    expected_summary.size = 2;
    assert_eq!(&qw.manager.queue_summary(queue_name).unwrap(), &expected_summary);

    qw.next_job();
    qw.next_job();
    qw.next_empty_job();
    qw.next_empty_job();
    expected_summary.size = 0;
    assert_eq!(&qw.manager.queue_summary(queue_name).unwrap(), &expected_summary);
}

#[test]
fn basic_summary() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::new(&conn, "none");
    let expected = ServerInfo::default();
    let summary = qw.manager.server_info().unwrap();
    assert_eq!(summary, expected);
}

#[test]
fn job_not_exists() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::new(&conn, "none");
    assert_eq!(qw.manager.job_fields(1, None), Err(OcyError::NoSuchJob(1)));
    assert_eq!(qw.manager.job_status(1), Err(OcyError::NoSuchJob(1)));
}

#[test]
fn job_creation() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let job_id = qw.new_default_job().id();
    let job_info = qw.job_meta(job_id);
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
    assert_eq!(qw.new_job(&job_req).id(), 2);
    let job_info = qw.job_meta(2);
    assert_eq!(job_info.id(), 2);
    assert_eq!(job_info.input(), job_req.input);
}

#[test]
fn job_output() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    assert_eq!(qw.manager.job_output(123), Err(OcyError::NoSuchJob(123)));
    // TODO: populate test
}

#[test]
fn job_deletion() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let tags = vec!["tag".to_string()];
    let job_req = job::CreateRequest { tags: Some(tags), ..Default::default() };

    let jobs = create_job_in_all_states(&qw.manager, DEFAULT_QUEUE, &job_req);
    let job_id_running = jobs[&job::Status::Running];
    let job_id_completed = jobs[&job::Status::Completed];
    let job_id_failed = jobs[&job::Status::Failed];
    let job_id_cancelled = jobs[&job::Status::Cancelled];
    let job_id_timed_out = jobs[&job::Status::TimedOut];
    let job_id_queued = jobs[&job::Status::Queued];

    assert_eq!(qw.manager.running_queue_size().unwrap(), 1);
    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_running));
    assert!(qw.manager.delete_job(job_id_running).unwrap());
    assert_eq!(qw.manager.running_queue_size().unwrap(), 0);
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_running));

    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_completed));
    assert!(qw.manager.delete_job(job_id_completed).unwrap());
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_completed));

    assert_eq!(qw.manager.failed_queue_size().unwrap(), 2);
    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_failed));
    assert!(qw.manager.delete_job(job_id_failed).unwrap());
    assert_eq!(qw.manager.failed_queue_size().unwrap(), 1);
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_failed));

    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_cancelled));
    assert!(qw.manager.delete_job(job_id_cancelled).unwrap());
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_cancelled));

    assert_eq!(qw.manager.failed_queue_size().unwrap(), 1);
    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_timed_out));
    assert!(qw.manager.delete_job(job_id_timed_out).unwrap());
    assert_eq!(qw.manager.failed_queue_size().unwrap(), 0);
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_timed_out));

    assert_eq!(qw.manager.queue_size(DEFAULT_QUEUE).unwrap(), 1);
    assert!(qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_queued));
    assert!(qw.manager.delete_job(job_id_queued).unwrap());
    assert_eq!(qw.manager.queue_size(DEFAULT_QUEUE).unwrap(), 0);
    assert!(!qw.manager.tagged_job_ids("tag").unwrap().contains(&job_id_queued));

    // ensure deleting non-existing jobs returns false
    assert!(!qw.manager.delete_job(job_id_running).unwrap());
    assert!(!qw.manager.delete_job(job_id_completed).unwrap());
    assert!(!qw.manager.delete_job(job_id_failed).unwrap());
    assert!(!qw.manager.delete_job(job_id_cancelled).unwrap());
    assert!(!qw.manager.delete_job(job_id_timed_out).unwrap());
    assert!(!qw.manager.delete_job(job_id_queued).unwrap());
}

#[test]
fn job_retry_no_queue() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: Some(Duration::from_secs(0)),
        expires_after: Some(Duration::from_secs(0)),
        retries: Some(3),
        retry_delays: None,
        ..Default::default() };

    // create and start new job running
    let job_id = qw.new_running_job(&job_req).id();
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st failure
    let job_info = qw.fail_job(job_id);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // delete job's original queue
    assert!(qw.manager.delete_queue(DEFAULT_QUEUE).unwrap());

    // 1st retry
    assert_eq!(qw.manager.check_job_retries().unwrap(), Vec::<u64>::new());
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 0);
}

#[test]
fn job_retries() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: Some(Duration::from_secs(0)),
        expires_after: Some(Duration::from_secs(0)),
        retries: Some(3),
        retry_delays: None,
        ..Default::default() };

    // create and start new job running
    let job_id = qw.new_running_job(&job_req).id();
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st failure
    let job_info = qw.fail_job(job_id);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), None);

    // 1st retry
    assert_eq!(qw.manager.check_job_retries().unwrap(), vec![job_id]);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 1);

    // ensure it doesn't get retried while already running
    let empty: Vec<u64> = Vec::new();
    assert_eq!(qw.manager.check_job_retries().unwrap(), empty);

    // 2nd failure: job timeout
    let job_id = qw.next_job().id();
    sleep(time::Duration::from_secs(2));
    assert_eq!(qw.manager.check_job_timeouts().unwrap(), vec![job_id]);
    assert_eq!(qw.job_status(job_id), job::Status::TimedOut);

    // 2nd retry
    assert_eq!(qw.manager.check_job_retries().unwrap(), vec![job_id]);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 2);

    // 3rd failure
    let job_id = qw.next_job().id();
    qw.fail_job(job_id);

    // 3rd retry
    assert_eq!(qw.manager.check_job_retries().unwrap(), vec![job_id]);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Queued);
    assert_eq!(job_info.retries_attempted(), 3);

    // fail final time
    let job_id = qw.next_job().id();
    qw.fail_job(job_id);

    // ensure it doesn't get retries any more
    assert_eq!(qw.manager.check_job_retries().unwrap(), empty);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 3);
}

#[test]
fn job_retry_delays() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

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
    let job_id = qw.new_running_job(&job_req).id();

    // 1st failure
    qw.fail_job(job_id);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries(), 3);
    assert_eq!(job_info.retries_attempted(), 0);
    assert_eq!(job_info.retry_delays(), Some(delays));

    // no retry yet
    assert_eq!(qw.manager.check_job_retries().unwrap(), empty);
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Failed);
    assert_eq!(job_info.retries_attempted(), 0);

    sleep(time::Duration::from_secs(2));
}

#[test]
fn tag_creation() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    assert_eq!(qw.new_default_job().tags(), None);

    // maintain list of all job IDs which have been given tag "foo"
    let mut foo_tagged = Vec::new();

    let tags = vec!["foo".to_string(),
                    "another-tag".to_string(),
                    "3rd.tag_example".to_string()];
    let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };
    let job_id = qw.new_job(&job_req).id();
    assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
    foo_tagged.push(job_id);

    // should get empty list for non-existent tags
    assert_eq!(qw.manager.tagged_job_ids("some_tag"), Ok(Vec::new()));

    // should get single job
    for tag in &tags {
        assert_eq!(qw.manager.tagged_job_ids(tag), Ok(vec![job_id]));
    }

    let tags = vec!["foo".to_string(), "bar".to_string()];
    let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };

    let job_id = qw.new_job(&job_req).id();
    assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
    foo_tagged.push(job_id);

    let job_id = qw.new_job(&job_req).id();
    assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
    foo_tagged.push(job_id);

    let job_id = qw.new_job(&job_req).id();
    assert_eq!(qw.job_meta(job_id).tags(), Some(tags.clone()));
    foo_tagged.push(job_id);

    foo_tagged.sort();
    assert_eq!(qw.manager.tagged_job_ids("foo"), Ok(foo_tagged));
}

#[test]
fn tag_deletion() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let tags = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    // ensure no job IDs are returned for non-existent tags
    for tag in &tags {
        assert_eq!(qw.manager.tagged_job_ids(tag), Ok(Vec::new()));
    }

    // ensure tag info returned for jobs, and job info returned for tags
    let job_req = job::CreateRequest { tags: Some(tags.clone()), ..Default::default() };
    let tagged_ids: Vec<u64> = (0..4).map(|_| {
        let job_id = qw.new_job(&job_req).id();
        assert_eq!(qw.manager.job_fields(job_id, None).unwrap().tags(), Some(tags.clone()));
        job_id
    }).collect();

    for tag in &tags {
        assert_eq!(qw.manager.tagged_job_ids(tag).unwrap(), tagged_ids);
    }

    // TODO: add when deletion actually added
}

#[test]
fn job_starting() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let input: serde_json::Value = vec![1, 2, 3].into();
    let mut job_req = job::CreateRequest::default();
    job_req.input = Some(input.clone());
    let job_id = qw.new_job(&job_req).id();

    let job_payload = qw.next_job();
    assert_eq!(job_payload.id(), job_id);
    assert_eq!(job_payload.input(), &Some(input.clone()));

    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.queue(), DEFAULT_QUEUE);
    assert_eq!(job_info.status(), job::Status::Running);
    assert!(job_info.started_at().is_some());
    assert!(job_info.ended_at().is_none());
    assert!(job_info.last_heartbeat().is_none());
    assert_eq!(job_info.input(), Some(input));
    assert!(job_info.output().is_none());
}

#[test]
fn job_fields() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let job_meta = qw.new_default_job();
    let job_id = job_meta.id();

    // single fields
    assert_eq!(qw.job_fields(job_id, &[job::Field::Id]).id(), 1);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Queue]).queue(), DEFAULT_QUEUE);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Status]).status(), job::Status::Queued);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Tags]).tags(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::CreatedAt]).created_at(), job_meta.created_at());
    assert_eq!(qw.job_fields(job_id, &[job::Field::StartedAt]).started_at(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::EndedAt]).ended_at(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::LastHeartbeat]).last_heartbeat(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Input]).input(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Output]).output(), None);
    assert_eq!(qw.job_fields(job_id, &[job::Field::Timeout]).timeout(), job_meta.timeout());
    assert_eq!(qw.job_fields(job_id, &[job::Field::HeartbeatTimeout]).heartbeat_timeout(), job_meta.heartbeat_timeout());
    assert_eq!(qw.job_fields(job_id, &[job::Field::ExpiresAfter]).expires_after(), job_meta.expires_after());
    assert_eq!(qw.job_fields(job_id, &[job::Field::Retries]).retries(), job_meta.retries());
    assert_eq!(qw.job_fields(job_id, &[job::Field::RetriesAttempted]).retries_attempted(), 0);
    assert_eq!(qw.job_fields(job_id, &[job::Field::RetryDelays]).retry_delays(), None);

    // multiple fields
    let jm = qw.job_fields(job_id, &[job::Field::Status, job::Field::Output]);
    assert_eq!(jm.status(), job_meta.status());
    assert_eq!(jm.output(), job_meta.output());

    let jm = qw.job_fields(job_id, &[job::Field::Id, job::Field::Tags, job::Field::RetryDelays]);
    assert_eq!(jm.id(), job_meta.id());
    assert_eq!(jm.tags(), job_meta.tags());
    assert_eq!(jm.retry_delays(), job_meta.retry_delays());

    // all fields
    let jm = qw.manager.job_fields(job_id, None).unwrap();
    assert_eq!(jm.id(), job_meta.id());
    assert_eq!(jm.status(), job_meta.status());
    assert_eq!(jm.output(), job_meta.output());
    assert_eq!(jm.tags(), job_meta.tags());
    assert_eq!(jm.retry_delays(), job_meta.retry_delays());

}

#[test]
fn update_job_heartbeat() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    qw.new_default_job();
    let job_id = qw.next_job().id();
    assert!(qw.job_meta(job_id).last_heartbeat().is_none());

    qw.manager.update_job_heartbeat(job_id).unwrap();
    let hb1 = qw.job_meta(job_id).last_heartbeat().unwrap();

    qw.manager.update_job_heartbeat(job_id).unwrap();
    let hb2 = qw.job_meta(job_id).last_heartbeat().unwrap();

    assert!(hb2 > hb1);
}

#[test]
fn update_job_output() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    // can only update jobs if they exist
    match qw.manager.set_job_output(21, &"foo".into()) {
        Err(OcyError::NoSuchJob(21)) => (),
        x                              => assert!(false, "Unexpected result: {:?}", x),
    }

    let job_id = qw.new_default_job().id();

    // can only update output for running jobs
    match qw.manager.set_job_output(job_id, &"foo".into()) {
        Err(OcyError::BadRequest(_)) => (),
        x                              => assert!(false, "Unexpected result: {:?}", x),
    }

    // ensure output is initially unset
    let job_id = qw.next_job().id();
    assert!(qw.job_meta(job_id).output().is_none());

    // ensure output is set
    qw.manager.set_job_output(job_id, &"foo".into()).unwrap();
    assert_eq!(qw.job_meta(job_id).output(), Some("foo".into()));

    // ensure output is overwritten
    let map = serde_json::from_str("{\"a\": 1, \"b\": 2}").unwrap();
    qw.manager.set_job_output(job_id, &map).unwrap();
    assert_eq!(qw.job_meta(job_id).output(), Some(map));
}

#[test]
fn queued_status_transitions() {
    // TODO: add transitions for retries:
    // Cancelled -> Queued
    // Failed -> Queued
    // TimedOut -> Queued
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    // cannot manually change from Queued, only `next_job` does this
    let job_id = qw.new_default_job().id();
    let not_allowed = &[job::Status::Queued,
                        job::Status::Running,
                        job::Status::Failed,
                        job::Status::Completed,
                        job::Status::TimedOut];
    for new_status in not_allowed {
        match qw.manager.set_job_status(job_id, new_status) {
            Err(OcyError::BadRequest(_)) => (),
            x => assert!(false, "Unexpected result when changing status Queued -> {}: {:?}", new_status, x),
        }
    }

    // queued jobs can be cancelled
    qw.manager.set_job_status(job_id, &job::Status::Cancelled).unwrap();
    let job_info = qw.job_meta(job_id);
    assert_eq!(job_info.status(), job::Status::Cancelled);
    assert!(job_info.ended_at().is_some());
}

#[test]
fn running_status_transitions() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let not_allowed = &[
        job::Status::Queued,   // cannot re-queue a running job
        job::Status::Running,  // job is already running
    ];
    let job_id = qw.new_running_default_job().id();
    for new_status in not_allowed {
        match qw.manager.set_job_status(job_id, new_status) {
            Err(OcyError::BadRequest(_)) => (),
            x => assert!(false, "Unexpected result when changing status Running -> {}: {:?}", new_status, x),
        }
    }

    let allowed = &[
        job::Status::Cancelled,
        job::Status::Completed,
        job::Status::Failed,
        job::Status::TimedOut, // TODO: should this be limited to being set by the server only?
    ];
    for new_status in allowed {
        let job_id = qw.new_running_default_job().id();
        qw.manager.set_job_status(job_id, new_status).unwrap();
        let job_info = qw.job_meta(job_id);
        assert_eq!(job_info.status(), *new_status);
        assert!(job_info.ended_at().is_some());
    }
}

#[test]
fn completed_status_transitions() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    // once completed, job status cannot be changed
    let job_id = qw.new_running_default_job().id();
    qw.complete_job(job_id);
    let not_allowed = &[
        job::Status::Completed,
        job::Status::Queued,
        job::Status::Running,
        job::Status::Failed,
        job::Status::Completed,
        job::Status::TimedOut
    ];
    for new_status in not_allowed {
        match qw.manager.set_job_status(job_id, new_status) {
            Err(OcyError::BadRequest(_)) => (),
            x => assert!(false, "Unexpected result when changing status Completed -> {}: {:?}", new_status, x),
        }
    }
}

#[test]
fn check_ping() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);
    qw.manager.check_ping().unwrap();
}

#[test]
fn job_heartbeat_timeout() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let mut job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(3600)),
        heartbeat_timeout: Some(Duration::from_secs(1)),
        expires_after: Some(Duration::from_secs(3600)),
        ..Default::default()
    };
    let job_id_a = qw.new_job(&job_req).id();

    job_req.heartbeat_timeout = Some(Duration::from_secs(3));
    let job_id_b = qw.new_job(&job_req).id();
    let job_id_c = qw.new_job(&job_req).id();

    qw.next_job();
    qw.next_job();
    qw.next_job();

    assert_eq!(qw.job_status(job_id_a), job::Status::Running);
    assert_eq!(qw.job_status(job_id_b), job::Status::Running);
    assert_eq!(qw.job_status(job_id_c), job::Status::Running);

    sleep(time::Duration::from_secs(2));
    assert_eq!(qw.manager.check_job_timeouts().unwrap(), vec![job_id_a]);
    assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
    assert_eq!(qw.job_status(job_id_b), job::Status::Running);
    assert_eq!(qw.job_status(job_id_c), job::Status::Running);

    qw.manager.update_job_heartbeat(job_id_c).unwrap();
    sleep(time::Duration::from_secs(2));
    assert_eq!(qw.manager.check_job_timeouts().unwrap(), vec![job_id_b]);
    assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
    assert_eq!(qw.job_status(job_id_b), job::Status::TimedOut);
    assert_eq!(qw.job_status(job_id_c), job::Status::Running);

    assert_eq!(qw.job_meta(job_id_a).last_heartbeat(), None);
    assert_eq!(qw.job_meta(job_id_b).last_heartbeat(), None);
    assert!(qw.job_meta(job_id_c).last_heartbeat().is_some());
}

#[test]
fn job_timeout() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let mut job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: Some(Duration::from_secs(3600)),
        expires_after: Some(Duration::from_secs(3600)),
        ..Default::default()
    };
    let job_id_a = qw.new_job(&job_req).id();

    job_req.timeout = Some(Duration::from_secs(3));
    let job_id_b = qw.new_job(&job_req).id();

    qw.next_job();
    qw.next_job();

    assert_eq!(qw.job_status(job_id_a), job::Status::Running);
    assert_eq!(qw.job_status(job_id_b), job::Status::Running);

    sleep(time::Duration::from_secs(2));
    assert_eq!(qw.manager.check_job_timeouts().unwrap(), vec![1]);
    assert_eq!(qw.job_status(job_id_a), job::Status::TimedOut);
    assert_eq!(qw.job_status(job_id_b), job::Status::Running);
}

#[test]
fn job_expiry() {
    let (_ctx, conn) = init();
    let qw = QueueWrapper::with_default_queue(&conn);

    let job_req = job::CreateRequest {
        timeout: Some(Duration::from_secs(3600)),
        heartbeat_timeout: Some(Duration::from_secs(3600)),
        expires_after: Some(Duration::from_secs(1)),
        retries: Some(0),
        ..Default::default()
    };

    let jobs = create_job_in_all_states(&qw.manager, DEFAULT_QUEUE, &job_req);
    let job_id_running = jobs[&job::Status::Running];
    let job_id_completed = jobs[&job::Status::Completed];
    let job_id_failed = jobs[&job::Status::Failed];
    let job_id_cancelled = jobs[&job::Status::Cancelled];
    let job_id_timed_out = jobs[&job::Status::TimedOut];
    let job_id_queued = jobs[&job::Status::Queued];

    // confirm all jobs are in correct queues
    assert_eq!(qw.manager.running_queue_size().unwrap(), 1);
    assert_eq!(qw.manager.failed_queue_size().unwrap(), 2); // one failed, one timed out
    assert_eq!(qw.manager.ended_queue_size().unwrap(), 2); // one completed, one cancelled
    assert_eq!(qw.queue_size(), 1);

    // jobs in ended queue (i.e. one completed and one cancelled) should expire
    let mut expected_expired = vec![job_id_completed, job_id_cancelled,];
    expected_expired.sort();
    sleep(time::Duration::from_secs(2));
    let mut expired = qw.manager.check_job_expiry().unwrap();
    expired.sort();
    assert_eq!(expired, expected_expired);

    // no retries, failed/timed out jobs should be moved to ended queue, then expire next call
    assert_eq!(qw.manager.check_job_retries().unwrap(), Vec::<u64>::new());
     let mut expected_expired = vec![job_id_failed, job_id_timed_out];
    expected_expired.sort();
    sleep(time::Duration::from_secs(2));
    let mut expired = qw.manager.check_job_expiry().unwrap();
    expired.sort();
    assert_eq!(expired, expected_expired);

    assert_eq!(qw.manager.job_status(job_id_running),   Ok(job::Status::Running));
    assert_eq!(qw.manager.job_status(job_id_completed), Err(OcyError::NoSuchJob(job_id_completed)));
    assert_eq!(qw.manager.job_status(job_id_failed),    Err(OcyError::NoSuchJob(job_id_failed)));
    assert_eq!(qw.manager.job_status(job_id_cancelled), Err(OcyError::NoSuchJob(job_id_cancelled)));
    assert_eq!(qw.manager.job_status(job_id_timed_out), Err(OcyError::NoSuchJob(job_id_timed_out)));
    assert_eq!(qw.manager.job_status(job_id_queued),    Ok(job::Status::Queued));
}

fn create_job_in_all_states(
    manager: &RedisManager,
    queue: &str,
    create_req: &job::CreateRequest
) -> HashMap<job::Status, u64> {
     let mut job_req: job::CreateRequest = create_req.clone();

    let job_id_running = manager.create_job(queue, &job_req).unwrap();
    let job_id_completed = manager.create_job(queue, &job_req).unwrap();
    let job_id_failed = manager.create_job(queue, &job_req).unwrap();
    let job_id_cancelled = manager.create_job(queue, &job_req).unwrap();

    job_req.timeout = Some(Duration::from_secs(1));
    let job_id_timed_out = manager.create_job(queue, &job_req).unwrap();
    let job_id_queued = manager.create_job(queue, &job_req).unwrap();

    let mut update_req = job::UpdateRequest::default();

    // leave 1st job as running
    manager.next_queued_job(queue).unwrap();

    // complete 2nd job
    manager.next_queued_job(queue).unwrap();
    update_req.status = Some(job::Status::Completed);
    manager.update_job(job_id_completed, &update_req).unwrap();

    // fail 3rd job
    manager.next_queued_job(queue).unwrap();
    update_req.status = Some(job::Status::Failed);
    manager.update_job(job_id_failed, &update_req).unwrap();

    // cancel 4th job
    manager.next_queued_job(queue).unwrap();
    update_req.status = Some(job::Status::Cancelled);
    manager.update_job(job_id_cancelled, &update_req).unwrap();

    // 5th job should timeout
    manager.next_queued_job(queue).unwrap();

    sleep(time::Duration::from_secs(2));
    assert_eq!(manager.check_job_timeouts().unwrap(), vec![job_id_timed_out]);

    assert_eq!(manager.job_status(job_id_running), Ok(job::Status::Running));
    assert_eq!(manager.job_status(job_id_completed), Ok(job::Status::Completed));
    assert_eq!(manager.job_status(job_id_failed), Ok(job::Status::Failed));
    assert_eq!(manager.job_status(job_id_cancelled), Ok(job::Status::Cancelled));
    assert_eq!(manager.job_status(job_id_timed_out), Ok(job::Status::TimedOut));
    assert_eq!(manager.job_status(job_id_queued), Ok(job::Status::Queued));

    let mut map = HashMap::with_capacity(6);
    map.insert(job::Status::Running, job_id_running);
    map.insert(job::Status::Completed, job_id_completed);
    map.insert(job::Status::Failed, job_id_failed);
    map.insert(job::Status::Cancelled, job_id_cancelled);
    map.insert(job::Status::TimedOut, job_id_timed_out);
    map.insert(job::Status::Queued, job_id_queued);
    map
}
