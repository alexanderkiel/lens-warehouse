(ns system
  (:require [clojure.string :as str]
            [org.httpkit.server :refer [run-server]]
            [lens.app :refer [app]]
            [lens.util :refer [parse-int]]))

(defn env []
  (->> (str/split-lines (slurp ".env"))
       (reduce (fn [ret line]
                 (let [vs (str/split line #"=")]
                   (assoc ret (first vs) (str/join "=" (rest vs))))) {})))

(defn system [env]
  {:app app
   :db-uri (or (env "DB_URI") "datomic:mem://lens")
   :version (System/getProperty "lens.version")
   :port (or (some-> (env "PORT") (parse-int)) 5001)
   :mdb-uri (env "MDB_URI")})

(defn start [{:keys [app db-uri version port] :as system}]
  (let [stop-fn (run-server (app db-uri version) {:port port})]
    (assoc system :stop-fn stop-fn)))

(defn stop [{:keys [stop-fn] :as system}]
  (stop-fn)
  (dissoc system :stop-fn))
