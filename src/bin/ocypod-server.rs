//! Main executable that runs the HTTP server for the queue application.

extern crate ocypod;
extern crate clap;
extern crate actix;
extern crate actix_web;
extern crate env_logger;
extern crate redis;
#[macro_use]
extern crate log;

use actix_web::{server, App, http, middleware::Logger};
use actix::prelude::*;
use ocypod::{config, models::ApplicationState};
use ocypod::actors::{application::ApplicationActor, monitor::MonitorActor};
use ocypod::handlers;

fn main() {
    // TODO: control logging from config file
    // initialise logging, controlled by environment variable, e.g. RUST_LOG=debug
    env_logger::init();

    // TODO: look for config in default locations, e.g. /etc/ocypod.toml, ~/.ocypod.toml, ./ocypod.toml
    // environment, etc., use config flag to specify non-default
    // parse CLI arguments and configuration file
    let config = parse_config_from_cli_args(&parse_cli_args());

    let http_server_addr = config.server_addr();
    let redis_url = config.redis_url();
    let redis_client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("Failed to initialise Redis client: {}", err);
            std::process::exit(1);
        }
    };

    // initialise Actix actor system
    let sys = actix::System::new("ocypod");

    // TODO: don't hardcode to 4, pick some other sensible value
    let num_workers = config.redis.threads.unwrap_or(4);

    // start N sync workers
    debug!("Starting {} Redis workers", num_workers);
    let redis_addr = SyncArbiter::start(num_workers, move || {
        ApplicationActor::new(redis_client.clone())
    });
    info!("{} Redis workers started", num_workers);

    // start actor that executes periodic tasks
    let _monitor_addr = MonitorActor::new(redis_addr.clone(), &config.server).start();

    // Use 0 to signal that default should be used. This configured the max size that POST endpoints
    // will accept.
    let max_body_size =  if let Some(size) = config.server.max_body_size {
        debug!("Setting max body size to {} bytes", size);
        size
    } else {
        0
    };

    // Set up HTTP server routing. Each endpoint has access to the ApplicationState struct, allowing them to send
    // messages to the RedisActor (which performs task queue operations).
    let config_copy = config.clone();
    let mut http_server = server::new(move || {
        let max_body_size = max_body_size.clone();

        App::with_state(ApplicationState::new(redis_addr.clone(), config_copy.clone()))
            // TODO: better endpoint name? Info? System? Status?
            // get a summary of the Ocypod system as a whole, e.g. number of jobs in queues, job states, etc.
            .scope("/info", |info_scope| {
                info_scope
                    // get current server version
                    .resource("/version", |r| r.f(handlers::info::version))

                    // get summary of system/queue information
                    .resource("", |r| r.f(handlers::info::index))
            })

            // run basic health check by pinging Redis
            .resource("/health", |r| r.f(handlers::health::index))

            // get list of job IDs for a given tag
            .resource("/tag/{name}", |r| r.method(http::Method::GET).with(handlers::tag::tagged_jobs))

            .scope("/job", |job_scope| {
                job_scope
                    // get the current status of a job with given ID
                    .resource("/{id}/status", |r| r.method(http::Method::GET).with(handlers::job::status))
                    .resource("/{id}/output", move |r| {
                        // TODO: deprecate this and just use fields endpoint?
                        // get the output field of a job with given ID
                        r.method(http::Method::GET).with(handlers::job::output);

                        // update the output field of a job with given ID
                        if max_body_size > 0 {
                            r.method(http::Method::PUT)
                                .with_config(handlers::job::set_output, |((_, cfg), _)| { cfg.limit(max_body_size); })
                        } else {
                            r.method(http::Method::PUT).with(handlers::job::set_output)
                        }
                    })

                    // update job's last heartbeat date/time
                    .resource("/{id}/heartbeat", |r| r.method(http::Method::PUT).with(handlers::job::heartbeat))

                    .resource("/{id}", move |r| {
                        // TODO: add endpoint to get multiple fields
                        // get all metadata about a single job with given ID
                        r.method(http::Method::GET).with(handlers::job::index);

                        // update one of more fields (including status) of given job
                        if max_body_size > 0 {
                            r.method(http::Method::PATCH)
                                .with_config(handlers::job::update, |((_, cfg), _)| { cfg.limit(max_body_size); });
                        } else {
                            r.method(http::Method::PATCH).with(handlers::job::update);
                        }

                        // delete a job from the queue DB
                        r.method(http::Method::DELETE).with(handlers::job::delete)
                    })
            })

            .scope("/queue", |queue_scope| {
                queue_scope
                    .resource("/{name}/job", move |r| {
                        let max_body_size = max_body_size.clone();

                        // get the next job to work on from given queue
                        r.method(http::Method::GET).with(handlers::queue::next_job);

                        // create a new job on given queue
                        if max_body_size > 0 {
                            r.method(http::Method::POST)
                                .with_config(handlers::queue::create_job, |((_, cfg), _)| { cfg.limit(max_body_size); })
                        } else {
                            r.method(http::Method::POST).with(handlers::queue::create_job)
                        }
                    })

                    .resource("/{name}", |r| {
                        // TODO: split into separate endpoints?
                        // get summary information about a given queue, size, settings, etc.
                        r.method(http::Method::GET).with(handlers::queue::queue_summary);

                        // create a new queue, or update an existing one with given settings
                        r.method(http::Method::PUT).with(handlers::queue::create_or_update_queue);

                        // delete a queue and all currently queued jobs on it
                        r.method(http::Method::DELETE).with(handlers::queue::delete_queue)
                    })

                    // get a list of all queue names
                    .resource("", |r| r.f(handlers::queue::index))
            })

            // add middleware logger for access log, if required
            .middleware(Logger::default())
    });

    // set number of worker threads if configured, or default to number of logical CPUs
    if let Some(num_workers) = config.server.threads {
        debug!("Using {} HTTP worker threads", num_workers);
        http_server = http_server.workers(num_workers);
    }

    http_server.bind(&http_server_addr)
        .expect("Failed to start HTTP server")
        .start();

    // start HTTP service and actor system
    info!("Starting queue server at: {}", &http_server_addr);
    let _ = sys.run();
}

/// Defines and parses CLI argument for this server.
fn parse_cli_args<'a>() -> clap::ArgMatches<'a> {
    clap::App::new("Ocypod")
        .version(handlers::info::VERSION)
        .arg(clap::Arg::with_name("config")
            .required(true)
            .help("Path to configuration file")
            .index(1))
        .get_matches()
}

/// Parses CLI arguments, finds location of config file, and parses config file into a struct.
fn parse_config_from_cli_args(matches: &clap::ArgMatches) -> config::Config {
    let config_path = matches.value_of("config").unwrap();
    match config::Config::from_file(config_path) {
        Ok(config) => config,
        Err(msg) => {
            eprintln!("Failed to parse config file {}: {}", config_path, msg);
            std::process::exit(1);
        },
    }
}
