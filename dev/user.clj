(ns user
  (:use plumbing.core)
  (:use criterium.core)
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.pprint :refer [pprint pp]]
            [clojure.repl :refer :all]
            [clojure.test :as test]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.core.reducers :as r]
            [clojure.core.cache :as cache]
            [clojure.core.async :as async :refer [chan go go-loop <! <!! >! alts! close!]]
            [lens.schema :as schema]
            [lens.routes :as routes]
            [lens.api :as api :refer :all]
            [datomic.api :as d]
            [system]
            [lens.util :as util]))

(def system nil)

(defn init []
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

(comment
  (startup)
  (reset))

(defn delete-database []
  (d/delete-database (:db-uri system)))

(defn create-database []
  (d/create-database (:db-uri system)))

(defn connect []
  (d/connect (:db-uri system)))

(defn study-event-col? [col]
  (and (= "Prolog" (:group-name col))
       (#{"gruppe" "grp"} (some-> (:alias col) (str/lower-case)))))

(defn count-datoms [db]
  (->> (d/datoms db :eavt)
       (r/map (constantly 1))
       (reduce +)))

(comment
  (time (count-datoms db)))

(comment
  (startup)
  (reset)
  (def conn (connect))
  (def db (d/db conn))

  )
