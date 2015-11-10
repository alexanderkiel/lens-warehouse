(ns system
  (:use plumbing.core)
  (:require [clojure.string :as str]
            [org.httpkit.server :refer [run-server]]
            [lens.app :refer [app]]
            [lens.util :refer [parse-long]]
            [datomic.api :as d]
            [lens.schema :as schema])
  (:import [java.io File]))

(defn env []
  (if (.canRead (File. ".env"))
    (->> (str/split-lines (slurp ".env"))
         (map #(str/split % #"="))
         (into {}))
    {}))

(defn create-mem-db []
  (let [uri "datomic:mem://lens"]
    (d/create-database uri)
    (schema/load-schema (d/connect uri))
    uri))

(defn system [env]
  {:app app
   :db-uri (or (env "DB_URI") (create-mem-db))
   :context-path (or (env "CONTEXT_PATH") "")
   :version (System/getProperty "lens-warehouse.version")
   :port (or (some-> (env "PORT") (parse-long)) 8080)})

(defnk start [app port & more :as system]
  (let [stop-fn (run-server (app more) {:port port})]
    (assoc system :stop-fn stop-fn)))

(defn stop [{:keys [stop-fn] :as system}]
  (stop-fn)
  (dissoc system :stop-fn))
