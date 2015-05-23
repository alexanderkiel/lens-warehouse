#!/usr/bin/env bash

lein with-profile production,datomic-free trampoline run -p 8080 -d ${DB_URI:-datomic:free://db:4334/lens-warehouse} -c ${CONTEXT_PATH:-/}
