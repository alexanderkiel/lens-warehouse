(ns lens.system
  (:use plumbing.core)
  (:require [com.stuartsierra.component :as comp]
            [lens.bus :as bus]
            [lens.tx-report :refer [new-tx-reporter]]
            [lens.server :refer [new-server]]
            [lens.search :refer [new-search-index-ingestor new-search-conn]]
            [lens.datomic :refer [new-database-creator]]
            [lens.util :as util]))

(defnk new-system [lens-warehouse-version context-path db-uri port
                   search-host {search-port "9200"} search-index]
  (comp/system-map
    :version lens-warehouse-version
    :context-path context-path
    :db-uri db-uri
    :port (util/parse-long port)
    :thread 4

    :search-conn
    (new-search-conn search-host (util/parse-long search-port) search-index)

    :db-creator
    (comp/using (new-database-creator) [:db-uri])

    :server
    (comp/using (new-server) [:version :db-uri :search-conn :port :thread
                              :context-path])

    :bus
    (bus/new-bus)

    :tx-reporter
    (comp/using (new-tx-reporter) [:bus :db-uri :db-creator])

    :search-index-ingestor
    (comp/using (new-search-index-ingestor) {:conn :search-conn :bus :bus})))
