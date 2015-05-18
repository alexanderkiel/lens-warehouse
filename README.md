# Lens Warehouse

The data warehouse component of the Lens system which is one of several backend
services.

## Usage

* lein with-profile production-run trampoline run

## Usage on Heroku Compatible PaaS

This application uses the following environment vars:

* `PORT` - the port to listen on
* `DB_URI` - the main Datomic database URI
* `CONTEXT_PATH` - an optional context path under which the workbook service runs

## Build

Currently a complete compilation works using:

    lein with-profile production compile :all

## Run

Just use the command from the Procfiles web task which currently is

    lein with-profile production-run trampoline run

Trampoline lets the Leiningen process behind. So this is a production ready run
command.

If you have foreman installed you can create an `.env` file listing the
environment vars specified above and just type `foreman start`.

## Develop

Running a REPL will load the user namespace. Use `(startup)` to start the server
and `(reset)` to reload after code changes.

## Using Datomic Pro

You need a license to be able to use the Pro Edition of Datomic. The Leiningen
project file contains two profiles, one for the Free Edition (datomic-free) and
one for the Pro Edition (datomic-pro).
