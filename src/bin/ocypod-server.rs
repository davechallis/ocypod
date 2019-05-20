//! Main executable that runs the HTTP server for the queue application.

use actix_web::{web, HttpServer, App};
use actix::prelude::{Actor, SyncArbiter};
use log::{debug, info, warn};
use num_cpus;

use ocypod::{config, models::ApplicationState};
use ocypod::actors::{application::ApplicationActor, monitor::MonitorActor};
use ocypod::handlers;

fn main() -> std::io::Result<()> {
    let config = parse_config_from_cli_args(&parse_cli_args());

    // TODO: is env_logger the best choice here, or would slog be preferable?
    {
        let log_settings = format!("ocypod={},ocypod-server={}", config.server.log_level, config.server.log_level);
        env_logger::Builder::new()
            .parse_filters(&log_settings)
            .default_format_module_path(false)
            .init();
        debug!("Log initialised using: {}", &log_settings);
    }

    let http_server_addr = config.server_addr();
    let redis_url = config.redis_url();
    let redis_client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("Failed to initialise Redis client: {}", err);
            std::process::exit(1);
        }
    };

    let num_workers = config.redis.threads.unwrap_or_else(num_cpus::get);

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
    let mut http_server = HttpServer::new(move || {

        let redis_client_copy = redis_client.clone();

        // start N sync workers
        debug!("Starting {} Redis worker(s)", num_workers);
        let redis_addr = SyncArbiter::start(num_workers, move || {
            ApplicationActor::new(redis_client_copy.clone())
        });
        info!("{} Redis worker(s) started", num_workers);

        // start actor that executes periodic tasks
        let _monitor_addr = MonitorActor::new(redis_addr.clone(), &config_copy.server).start();

        App::new()
            // add middleware logger for access log, if required
            .wrap(actix_web::middleware::Logger::default())
            .data(web::JsonConfig::default().limit(
                if max_body_size > 0 {
                    max_body_size
                } else {
                    1024
                }
            ))

            .data(ApplicationState::new(redis_addr.clone(), config_copy.clone()))
            // get a summary of the Ocypod system as a whole, e.g. number of jobs in queues, job states, etc.
            .service(
                web::scope("/info")
                    // get current server version
                    .service(web::resource("/version").to(handlers::info::version))

                    // get summary of system/queue information
                    .service(web::resource("").to_async(handlers::info::index))
            )

            // run basic health check by pinging Redis
            .service(web::resource("/health").to_async(handlers::health::index))

            // get list of job IDs for a given tag
            .service(
                web::resource("/tag/{name}")
                    .route(web::get().to_async(handlers::tag::tagged_jobs)))

            .service(
                web::scope("/job")
                    // get the current status of a job with given ID
                    .service(web::resource("/{id}/status").route(web::get().to_async(handlers::job::status)))

                    .service(
                        web::resource("/{id}/output")
                            .route(web::get().to_async(handlers::job::set_output))
                            .route(web::put().to_async(handlers::job::set_output))
                    )

                    // update job's last heartbeat date/time
                    .service(web::resource("/{id}/heartbeat").route(web::put().to_async(handlers::job::heartbeat)))

                    .service(
                        web::resource("/{id}")
                            // get all metadata about a single job with given ID
                            .route(web::get()).to_async(handlers::job::index)

                            // update one of more fields (including status) of given job
                            .route(web::patch().to_async(handlers::job::update))

                            // delete a job from the queue DB
                            .route(web::delete()).to_async(handlers::job::delete)
                    )
            )

            .service(
                web::scope("/queue")
                    .service(
                        web::resource("/{name}/job")
                            // get the next job to work on from given queue
                            .route(web::get()).to_async(handlers::queue::next_job)

                            // create a new job on given queue
                            .route(web::post().to_async(handlers::queue::create_job))
                    )

                    // get queue size
                    .service(web::resource("/{name}/size").route(web::get()).to_async(handlers::queue::size))

                    .service(
                        web::resource("/{name}")
                            .route(web::get()).to_async(handlers::queue::settings)

                            // create a new queue, or update an existing one with given settings
                            .route(web::put()).to_async(handlers::queue::create_or_update)

                            // delete a queue and all currently queued jobs on it
                            .route(web::delete()).to_async(handlers::queue::delete)
                    )

                    // get a list of all queue names
                    .service(web::resource("").to_async(handlers::queue::index))
            )
    });

    // set number of worker threads if configured, or default to number of logical CPUs
    if let Some(num_workers) = config.server.threads {
        debug!("Using {} HTTP worker threads", num_workers);
        http_server = http_server.workers(num_workers);
    }

    if let Some(dur) = &config.server.shutdown_timeout {
        debug!("Setting shutdown timeout to {}", dur);
        http_server = http_server.shutdown_timeout(dur.as_secs());
    }

    // start HTTP service and actor system
    info!("Starting queue server at: {}", &http_server_addr);

    http_server.bind(&http_server_addr)
        .unwrap_or_else(|_| panic!("Failed to bind to: {}", &http_server_addr))
        .run()
}

/// Defines and parses CLI argument for this server.
fn parse_cli_args<'a>() -> clap::ArgMatches<'a> {
    clap::App::new("Ocypod")
        .version(handlers::info::VERSION)
        .arg(clap::Arg::with_name("config")
            .required(false)
            .help("Path to configuration file")
            .index(1))
        .get_matches()
}

/// Parses CLI arguments, finds location of config file, and parses config file into a struct.
fn parse_config_from_cli_args(matches: &clap::ArgMatches) -> config::Config {
    let conf = match matches.value_of("config") {
        Some(config_path) => {
            match config::Config::from_file(config_path) {
                Ok(config) => config,
                Err(msg) => {
                    eprintln!("Failed to parse config file {}: {}", config_path, msg);
                    std::process::exit(1);
                },
            }
        },
        None => {
            warn!("No config file specified, using default config");
            config::Config::default()
        }
    };

    // validate config settings
    if let Some(dur) = &conf.server.shutdown_timeout {
        if dur.as_secs() > std::u16::MAX.into() {
            eprintln!("Maximum shutdown_timeout is {} seconds", std::u16::MAX);
            std::process::exit(1);
        }
    }

    conf
}
