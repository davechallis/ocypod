use std::collections::HashMap;
use actix_web::{web, App, HttpServer};
use deadpool_redis::Pool;
use log::{debug, info};

use ocypod::handlers;
use ocypod::application::RedisManager;
use ocypod::models::OcyError;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse CLI config, or exit with non-zero status code on error.
    let config = ocypod::config::parse_config_from_cli_args();

    // TODO: is env_logger the best choice here, or would slog be preferable?
    {
        let log_settings = format!(
            "ocypod={},ocypod-server={}",
            config.server.log_level, config.server.log_level
        );
        env_logger::Builder::new()
            .parse_filters(&log_settings)
            .format_module_path(false)
            .init();
        debug!("Log initialised using: {}", &log_settings);
    }

    let redis_url = config.redis_url();

    let pool = {
        let pool_cfg = deadpool_redis::Config::from_url(redis_url);
        match pool_cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)) {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("Failed to initialise Redis connection pool: {}", err);
                std::process::exit(1);
            }
        }
    };
    debug!("Initialised Redis connection pool to {}", redis_url);

    let http_server_addr = config.server_addr();
    let redis_manager = RedisManager::new(&config.redis.key_namespace);

    // Create/update any queues found in the config file, unless they already exist with the same settings.
    if let Err(err) = create_queues_from_config(&pool, &redis_manager, &config.queue).await {
        eprintln!("Failed to initialise queues from configuration file: {}", err);
        std::process::exit(1);
    }

    let app_state = web::Data::new(ocypod::models::ApplicationState {
        pool: pool.clone(),
        config: config.clone(),
        redis_manager: redis_manager.clone(),
    });

    // Use 0 to signal that default should be used. This configured the max size that POST endpoints
    // will accept.
    let max_body_size = if let Some(size) = config.server.max_body_size {
        debug!("Setting max body size to {} bytes", size);
        size
    } else {
        0
    };

    let mut http_server = HttpServer::new(move || {
        App::new()
            // add middleware logger for access log, if required
            .wrap(actix_web::middleware::Logger::default())
            .app_data(app_state.clone())
            .app_data(web::JsonConfig::default().limit(if max_body_size > 0 {
                max_body_size
            } else {
                1024
            }))
            .service(
                web::scope("/info")
                    // get current server version
                    .service(web::resource("/version").to(handlers::info::version))
                    // get summary of system/queue information
                    .service(web::resource("").to(handlers::info::index)),
            )
            // Run basic health check by PINGing Redis.
            .route("/health", web::get().to(handlers::health::index))
            // Get list of job IDs for a given tag.
            .route("/tag/{name}", web::get().to(handlers::tag::tagged_jobs))
            .service(
                web::scope("/job")
                    // Get current status of job with given ID.
                    .service(web::resource("/{id}/status").to(handlers::job::status))
                    // Get or set a job's output.
                    .service(
                        web::resource("/{id}/output")
                            .route(web::get().to(handlers::job::output))
                            .route(web::put().to(handlers::job::set_output)),
                    )
                    // Update a job's last heartbeat date/time.
                    .service(
                        web::resource("/{id}/heartbeat")
                            .route(web::put().to(handlers::job::heartbeat)),
                    )
                    .service(
                        web::resource("/{id}")
                            // Get all metadata about a job with given ID.
                            .route(web::get().to(handlers::job::index))
                            // Update one or more fields (including status) of a job.
                            .route(web::patch().to(handlers::job::update))
                            // Delete a job from the queue DB.
                            .route(web::delete().to(handlers::job::delete)),
                    ),
            )
            .service(
                web::scope("/queue")
                    // Job IDs by state.
                    .service(web::resource("/{name}/job_ids").to(handlers::queue::job_ids))
                    .service(
                        web::resource("/{name}/job")
                            // Get the next job to work on from given queue.
                            .route(web::get().to(handlers::queue::next_job))
                            // Create a new job on given queue.
                            .route(web::post().to(handlers::queue::create_job)),
                    )
                    // Get queue size.
                    .service(web::resource("/{name}/size").to(handlers::queue::size))
                    .service(
                        web::resource("/{name}")
                            // Get queue's settings.
                            .route(web::get().to(handlers::queue::settings))
                            // Create a new queue, or update an existing one with given settings.
                            .route(web::put().to(handlers::queue::create_or_update))
                            // Delete a queue and all currently queued jobs on it.
                            .route(web::delete().to(handlers::queue::delete)),
                    )
                    // Get a list of all queue names.
                    .service(web::resource("").to(handlers::queue::index)),
            )
    })
    .bind(&http_server_addr)?;

    // set number of worker threads if configured, or default to number of logical CPUs
    if let Some(num_workers) = config.server.threads {
        debug!("Using {} HTTP worker threads", num_workers);
        http_server = http_server.workers(num_workers);
    }

    if let Some(dur) = &config.server.shutdown_timeout {
        debug!("Setting shutdown timeout to {}", dur);
        http_server = http_server.shutdown_timeout(dur.as_secs());
    }

    debug!("Starting background monitor tasks");
    ocypod::application::monitor::start_monitors(&redis_manager, pool, &config.server);

    // Start HTTP server.
    info!("Starting queue server at: {}", &http_server_addr);
    http_server.run().await
}

/// Creates any queues found in
async fn create_queues_from_config(
    pool: &Pool,
    redis_manager: &RedisManager,
    queues: &Option<HashMap<String, ocypod::models::queue::Settings>>,
) -> ocypod::models::OcyResult<()> {
    let queues = match queues {
        Some(queues) => queues,
        None => return Ok(()),
    };

    let mut conn = pool.get().await?;

    debug!("Ensuring that {} queue(s) from configuration file exist", queues.len());
    for (name, settings) in queues {
        match redis_manager.queue_settings(&mut conn, name).await {
            Ok(ref existing_settings) => {
                if settings != existing_settings {
                    redis_manager.create_or_update_queue(&mut conn, name, settings).await?;
                } else {
                    debug!("Queue \"{}\" already exists with configured settings, nothing to create", name);
                }
            },
            Err(OcyError::NoSuchQueue(_)) => {
                redis_manager.create_or_update_queue(&mut conn, name, settings).await?;
            },
            Err(err) => return Err(err),
        }
    }
    Ok(())
}