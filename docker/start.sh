#!/usr/bin/env bash

lein with-profile production,datomic-${DATOMIC_EDITION:-free} trampoline run
