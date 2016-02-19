(ns lens.core
  (:use plumbing.core)
  (:require [com.stuartsierra.component :as comp]
            [environ.core :refer [env]]
            [lens.system :refer [new-system]]
            [lens.logging :refer [info]]))

(defn- max-memory []
  (quot (.maxMemory (Runtime/getRuntime)) (* 1024 1024)))

(defn- available-processors []
  (.availableProcessors (Runtime/getRuntime)))

(defn -main [& _]
  (letk [[port thread version db-uri context-path :as system] (new-system env)]
    (comp/start system)
    (info {:version version})
    (info {:max-memory (max-memory)})
    (info {:num-cpus (available-processors)})
    (info {:datomic db-uri})
    (info {:context-path context-path})
    (info {:listen (str "0.0.0.0:" port)})
    (info {:num-worker-threads thread})))
