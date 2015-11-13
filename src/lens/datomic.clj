(ns lens.datomic
  (:require [com.stuartsierra.component :refer [Lifecycle]]
            [datomic.api :as d]
            [lens.schema :refer [load-schema]]))

(defrecord DatabaseCreator [db-uri]
  Lifecycle
  (start [creator]
    (when (d/create-database db-uri)
      (load-schema (d/connect db-uri)))
    creator)
  (stop [creator]
    creator))

(defn new-database-creator
  "Ensures that the database at db-uri exists."
  []
  (map->DatabaseCreator {}))
