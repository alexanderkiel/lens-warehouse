(ns lens.middleware.datomic
  (:require [datomic.api :as d]
            [schema.core :as s]
            [schema.coerce :as c]
            [schema.utils :as su]))

(defn- assoc-conn
  [request conn]
  (if (#{:post :put :delete} (:request-method request))
    (assoc request :conn conn)
    request))

(def ^:private int-coercer (c/coercer s/Int c/string-coercion-matcher))

(defn- as-of-t [{:keys [headers]}]
  (some-> (headers "x-datomic-as-of-t") (int-coercer)))

(defn- db [conn as-of-t]
  (if as-of-t
    (d/as-of (d/db conn) as-of-t)
    (d/db conn)))

(defn- assoc-db [request conn as-of-t]
  (assoc request :db (db conn as-of-t)))

(defn- connect [uri]
  (try
    (d/connect uri)
    (catch Throwable e
      (str "Error connecting to " uri ": " (.getMessage e)))))

(defn- connection-error-resp [conn]
  {:status 503
   :body
   {:data
    {:message conn}}})

(defn as-of-t-error-resp [error]
  {:status 400
   :body
   {:data
    {:message (str "Invalid X-Datomic-As-Of-T value: " (.-value error))}}})

(defn wrap-connection
  "Middleware which adds a connection and for GET requests a database to the
   params map.

   The connection is added to every request under the conn key.
   The database is added to GET requests under the db key."
  [handler uri]
  (fn [request]
    (let [conn (connect uri)]
      (if (string? conn)
        (connection-error-resp conn)
        (let [as-of-t (as-of-t request)]
          (if (su/error? as-of-t)
            (as-of-t-error-resp (su/error-val as-of-t))
            (-> request
                (assoc-conn conn)
                (assoc-db conn as-of-t)
                (handler))))))))
