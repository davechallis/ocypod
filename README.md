# ocypod

[![Build Status](https://travis-ci.com/davechallis/ocypod.svg?branch=master)](https://travis-ci.com/davechallis/ocypod)
[![Build status](https://ci.appveyor.com/api/projects/status/yi6kac4bn674upn1?svg=true)](https://ci.appveyor.com/project/davechallis/ocypod)
[![Documentation Status](https://readthedocs.org/projects/ocypod/badge/?version=latest)](https://ocypod.readthedocs.io/en/latest/?badge=latest)
[![Docker release](https://img.shields.io/docker/v/davechallis/ocypod?sort=semver)](https://hub.docker.com/r/davechallis/ocypod)


Ocypod is a language-agnostic, Redis-backed job queue server with an HTTP interface and a focus on long running tasks.

## Features

* simple setup - only requirement is Redis
* language agnostic - uses HTTP/JSON protocol, clients/workers can be
  implemented in any language
* long running jobs - handle jobs that may be running for hours/days,
  detect failure early using heartbeats
* simple HTTP interface - no complex binary protocols or client/worker logic
* flexible job metadata - allows for different patterns of use (e.g. progress
  tracking, partial results, etc.)
* job inspection - check the status of any jobs submitted to the system
* tagging - custom tags allow easy grouping and searching of related jobs
* automatic retries - re-queue jobs on failure or timeout

## Documentation

* [User guide and API documentation](https://ocypod.readthedocs.io/en/latest/)

## Contributing

Ocypod clients for various languages would be very welcome, will be happy to link to any I hear about.
