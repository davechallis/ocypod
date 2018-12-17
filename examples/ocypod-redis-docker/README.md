# Example Docker Compose

This directory contains an example configuration for running Ocypod using
[Docker Compose](https://docs.docker.com/compose/).

A fully working system can be started by running:

    $ docker-compose up

(or `docker-compose up -d` to run in the background)

This starts up two containers:

1. Redis (using a [Docker volume](https://docs.docker.com/storage/volumes/) for data persistence)
2. Ocypod (using `ocypod.toml` as its configuration file)

Once these are up and running, you can use `curl` to check that everything is
working as intended with:

    $ curl localhost:8023/health

For more details on how to get started with Ocypod, see the
[Quickstart](https://ocypod.readthedocs.io/en/latest/quickstart/) documentation.
