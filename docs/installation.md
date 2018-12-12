# Installation

## Prerequisites

### Redis

Ocypod uses [Redis](https://redis.io/) as a storage backend, and it's
recommended to use a dedicated Redis instance (or at least its own DB) to
avoid any clashes with other key spaces.

The latest [stable release](https://redis.io/download) (5.0) is recommended,
though there are no known issues using 4.0 or 3.2.

For development purposes, you can use `docker-compose up` in order to run a local redis instance.

## Building from source

Ocypod is written in [Rust](https://www.rust-lang.org/en-US/), so you'll need a [Rust installation](https://www.rust-lang.org/en-US/install.html) to build from source. Ocypod is generally built/tested using the latest stable Rust compiler release.

To build:

    $ git clone https://github.com/davechallis/ocypod.git
    $ cd ocypod
    $ cargo build --release

Check built executable:

    $ ./target/release/ocypod-server --version
