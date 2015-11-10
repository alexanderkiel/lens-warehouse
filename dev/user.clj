(ns user
  (:use plumbing.core)
  (:use criterium.core)
  (:require [clojure.pprint :refer [pprint pp]]
            [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :refer [refresh]]
            [lens.schema :as schema]
            [lens.api :as api]
            [datomic.api :as d]
            [system]))

(def system nil)

(defn init []
  (assert (nil? system))
  (alter-var-root #'system (constantly (system/system (system/env)))))

(defn start []
  (alter-var-root #'system system/start))

(defn stop []
  (alter-var-root #'system system/stop))

(defn startup []
  (init)
  (start)
  (println "Server running at port" (:port system)))

(defn reset []
  (stop)
  (refresh :after 'user/startup))

(defn create-database []
  (d/create-database (:db-uri system)))

(defn connect []
  (d/connect (:db-uri system)))

(defn load-schema []
  (keys (schema/load-schema (connect))))

;; Init Development
(comment
  (startup)
  (create-database)
  (load-schema)
  )

;; Reset after making changes
(comment
  (reset)
  )

;; Connection and Database in the REPL
(comment
  (def conn (connect))
  (def db (d/db conn))
  )
