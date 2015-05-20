__This software is ALPHA, lacks documentation and has to be deployed in conjunction with other Lens modules.__

# Lens Warehouse Service

The data warehouse component of the Lens system which is one of several backend
services.

## Usage with Leiningen

To start the service with leiningen, run the following command

    lein with-profile production,datomic-free trampoline run -h

This prints a help of all command line options. You need to specify at least a
database URI. The database URI has to point to a Datomic Free Edition Database.
If you like to use the Pro Edition, you have to use the `datomic-pro` leiningen
profile instead of the `datomic-free` profile. An example database URI is:
              
    datomic:free://localhost:4334/lens-warehouse

## Usage on Heroku Compatible PaaS

This application uses the following environment vars:

* `PORT` - the port to listen on
* `DB_URI` - the Datomic database URI
* `CONTEXT_PATH` - an optional context path under which the workbook service runs
* `DATOMIC_EDITION` - one of `free` or `pro` with a default of `free`

If you have [foreman][1] installed you can create an `.env` file listing the
environment vars specified above and just type `foreman start`.

## Develop

Running a REPL will load the user namespace. Use `(startup)` to start the server
and `(reset)` to reload after code changes.

## Using Datomic Pro

You need a license to be able to use the Pro Edition of Datomic. The Leiningen
project file contains two profiles, one for the Free Edition (datomic-free) and
one for the Pro Edition (datomic-pro).

## License

Copyright © 2015 Alexander Kiel

Distributed under the Eclipse Public License, the same as Clojure.

[1]: https://github.com/ddollar/foreman
