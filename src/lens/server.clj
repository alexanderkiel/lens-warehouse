(ns lens.server
  (:require [com.stuartsierra.component :refer [Lifecycle]]
            [org.httpkit.server :refer [run-server]]
            [lens.app :refer [app]]
            [lens.route :refer [ContextPath]]))

(defrecord Server [version context-path db-uri search-conn port thread stop-fn]
  Lifecycle
  (start [server]
    (let [handler (app {:version version
                        :db-uri db-uri
                        :search-conn search-conn
                        :context-path context-path})
          opts {:port port :thread thread}]
      (assoc server :stop-fn (run-server handler opts))))
  (stop [server]
    (stop-fn)
    (assoc server :stop-fn nil)))

(defn new-server []
  (map->Server {}))
