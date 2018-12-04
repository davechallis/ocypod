# ocypod

[![Build Status](https://travis-ci.com/davechallis/ocypod.svg?branch=master)](https://travis-ci.com/davechallis/ocypod)
[![Build status](https://ci.appveyor.com/api/projects/status/yi6kac4bn674upn1?svg=true)](https://ci.appveyor.com/project/davechallis/ocypod)

Ocypod is a language-agnostic, Redis-backed job queue server with an HTTP interface and a focus on long running tasks.

## Features

* language agnostic - uses HTTP/JSON protocol, clients/workers can be implemented in any language
* long running jobs - handle jobs that may be running for hours/days, detect failure early using heartbeats
* simple HTTP interface - no complex binary protocols or client/worker logic
* flexible job metadata - allows for different patterns of use (e.g. progress tracking, partial results, etc.)
* job inspection - check the status of any jobs submitted to the system
* tagged jobs - custom tags allow easy grouping and searching of related jobs

## Documentation

* [User guide and API documentation](https://davechallis.github.io/ocypod)
