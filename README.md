__Please start at the [Top-Level Lens Repo][6].__

# Lens Warehouse Service

[![Build Status](https://travis-ci.org/alexanderkiel/lens-warehouse.svg?branch=master)](https://travis-ci.org/alexanderkiel/lens-warehouse)

The data warehouse service of the Lens system which is one of several backend
services. The data warehouse service holds the medical study data which can be
queried.

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

## Usage through Docker Container

You have to start a database volume container, a database container and a 
warehouse container linking them all together:

    docker run --name lens-warehouse-db-vol akiel/lens-warehouse:db-latest
    docker run -d --volumes-from lens-warehouse-db-vol -e ALT_HOST=lens-warehouse-db --name lens-warehouse-db akiel/datomic-free
    docker run -d -p 8080:80 --link lens-warehouse-db:db --name lens-warehouse akiel/lens-warehouse

After starting all containers, a `curl http://localhost:8080` should show the
service document of Lens Warehouse. 

## Develop

Running a REPL will load the user namespace. Use `(startup)` to start the server
and `(reset)` to reload after code changes.

If you use [Intellij IDEA][2] with [Cursive][3], you can add a Datomic stub JAR
to your project dependencies as described [here][4]. The stub will provide
signatures and documentation for the Datomic API functions. I can't add the
stub dependency to the project.clj file because it is currently not available on
Clojars. I opened an issue [here][5].

## Using Datomic Pro

You need a license to be able to use the Pro Edition of Datomic. The Leiningen
project file contains two profiles, one for the Free Edition (datomic-free) and
one for the Pro Edition (datomic-pro).

## License

Copyright © 2015 Alexander Kiel

Distributed under the Eclipse Public License, the same as Clojure.

[1]: <https://github.com/ddollar/foreman>
[2]: <https://www.jetbrains.com/idea/>
[3]: <https://cursiveclojure.com>
[4]: <https://cursiveclojure.com/userguide/support.html>
[5]: <https://github.com/cursiveclojure/cursive/issues/896>
[6]: <https://github.com/alexanderkiel/lens>
