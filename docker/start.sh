#!/usr/bin/env bash

lein with-profile production,datomic-${DATOMIC_EDITION:-free} trampoline run -p 80 -d ${DB_URI:-datomic:free://db:4334/lens-warehouse} -c ${CONTEXT_PATH:-/}
