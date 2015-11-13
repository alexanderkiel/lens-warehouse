(ns lens.tx-report
  "Datomic Transaction Reporter

  Publishes a report of every transaction under :tx-report on the event bus."
  (:use plumbing.core)
  (:require [clojure.core.async :as async :refer [go go-loop alts!]]
            [lens.logging :refer [info debug]]
            [lens.bus :as bus]
            [datomic.api :as d]
            [com.stuartsierra.component :refer [Lifecycle]])
  (:import [java.util.concurrent BlockingQueue TimeUnit]))

(def ^:private sec TimeUnit/SECONDS)

(defrecord TxReporter [db-uri bus stop-ch]
  Lifecycle
  (start [this]
    (let [stop-ch (async/chan)
          ^BlockingQueue q (d/tx-report-queue (d/connect db-uri))]
      (info "Start listening on Datomic TX report queue...")
      (go-loop []
        (let [[val port] (alts! [stop-ch (go (.poll q 1 sec))])]
          (if (= port stop-ch)
            (info "End listening on Datomic TX report queue.")
            (do (when val
                  (debug "Publish TX report")
                  (bus/publish! bus :tx-report val))
                (recur)))))
      (assoc this :stop-ch stop-ch)))
  (stop [this]
    (d/remove-tx-report-queue (d/connect db-uri))
    (async/close! stop-ch)
    (assoc this :stop-ch nil)))

(defn new-tx-reporter []
  (map->TxReporter {}))
