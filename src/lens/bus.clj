(ns lens.bus
  (:use plumbing.core)
  (:require [clojure.core.async :as async :refer [go-loop <!]]
            [com.stuartsierra.component :refer [Lifecycle]]))

(defrecord Bus [publisher publication]
  Lifecycle
  (start [bus]
    (let [publisher (async/chan)]
      (assoc bus
        :publisher publisher
        :publication (async/pub publisher :topic))))
  (stop [bus]
    (async/close! publisher)
    (assoc bus :publisher nil :publication nil)))

(defn new-bus []
  (map->Bus {}))

(defn publish! [bus topic msg]
  (async/put! (:publisher bus) {:topic topic :msg msg}))

(defn listen-on
  "Listens on topic and calls the callback with the message.

  Returns a token which can be used to unlisten."
  [bus topic callback]
  (let [ch (async/chan)]
    (async/sub (:publication bus) topic ch)
    (go-loop []
      (when-letk [[msg] (<! ch)]
        (callback msg)
        (recur)))
    {:topic topic :chan ch}))

(defn unlisten [bus token]
  "Unlistens from a subscription described by token."
  (letk [[topic chan] token]
    (async/unsub (:publication bus) topic chan)))
