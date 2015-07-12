(ns lens.middleware.datomic
  (:require [datomic.api :as d]))

(defn- assoc-conn
  [request conn]
  (if (#{:post :put :delete} (:request-method request))
    (assoc request :conn conn)
    request))

(defn- assoc-db
  [request conn]
  (assoc request :db (d/db conn)))

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
        {:status 503
         :body
         {:data
          {:message conn}}}
        (let [request (-> request (assoc-conn conn) (assoc-db conn))]
          (handler request))))))
