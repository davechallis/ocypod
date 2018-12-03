//! Job queue system designed for long running tasks.

use std::alloc::System;

#[global_allocator]
static GLOBAL: System = System;

extern crate actix;
extern crate actix_web;
extern crate chrono;
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate human_size;
extern crate humantime;
#[macro_use]
extern crate log;
extern crate redis;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_humantime;
extern crate tokio_timer;
extern crate toml;

pub mod actors;
pub mod config;
pub mod handlers;
pub mod models;
pub mod application;
pub mod redis_utils;
