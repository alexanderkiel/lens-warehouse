(ns lens.middleware.datomic
  (:require [datomic.api :as d]
            [lens.api :as api])
  (:import [java.util UUID]))

(defn- assoc-conn
  [request conn]
  (if (#{:post :put :delete} (:request-method request))
    (assoc-in request [:params :conn] conn)
    request))

(defn- db [request conn]
  (let [db (d/db conn)]
    (if-let [snapshot (get-in request [:headers "x-lens-snapshot"])]
      (if-let [snapshot (api/snapshot db (UUID/fromString snapshot))]
        (d/as-of db (:db/id snapshot))
        db)
      db)))

(defn- assoc-db
  [request conn]
  (if (#{:get :head} (:request-method request))
    (assoc-in request [:params :db] (db request conn))
    request))

(defn- connect [uri]
  (try
    (d/connect uri)
    (catch Throwable e
      (str "Error connecting to " uri ": " (.getMessage e)))))

(defn add-cache-headers [req resp]
  (if-let [t (some-> req :params :db d/as-of-t)]
    (-> (assoc-in resp [:headers "cache-control"] "max-age=86400")
        (assoc-in [:headers "etag"] t)
        (assoc-in [:headers "vary"] "Accept, X-Lens-Snapshot"))
    resp))

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
         :body conn}
        (let [request (-> request (assoc-conn conn) (assoc-db conn))
              resp (handler request)]
          (add-cache-headers request resp))))))
