(ns lens.middleware.datomic
  (:require [datomic.api :as d]))

(defn- assoc-conn
  [request conn]
  (assoc-in request [:params :conn] conn))

(defn- assoc-db
  [request kw conn]
  (assoc-in request [:params kw] (d/db conn)))

(defn- connect [uri]
  (try
    (d/connect uri)
    (catch Throwable e
      (str "Error connecting to " uri ": " (.getMessage e)))))

(defn wrap-connection
  "Middleware which adds a connection and for GET requests a database to the
   params map.

   The connection is added to every request under the conn key.
   The database is added to GET requests under the db key."
  [handler uri]
  (fn [request]
    (let [conn (connect uri)]
      (if (string? conn)
        {:status 500
         :body conn}
        (-> request
            (assoc-conn conn)
            (assoc-db :db conn)
            (handler))))))
