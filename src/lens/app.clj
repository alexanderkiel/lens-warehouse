(ns lens.app
  (:use plumbing.core)
  (:require [bidi.bidi :as bidi]
            [bidi.ring :as bidi-ring]
            [io.clojure.liberator-transit]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [lens.route :refer [routes]]
            [lens.handler :refer [handlers]]
            [lens.representation]
            [lens.middleware.datomic :refer [wrap-connection]]
            [lens.middleware.wan-exception :refer [wrap-exception]]
            [lens.middleware.cors :refer [wrap-cors]]))

(defn path-for [routes]
  (fn [handler & params]
    (apply bidi/path-for routes handler params)))

(defn wrap-path-for [handler path-for]
  (fn [req] (handler (assoc req :path-for path-for))))

(defn wrap-not-found [handler]
  (fn [req]
    (if-let [resp (handler req)]
      resp
      {:status 404})))

(defnk app [db-uri context-path :as opts]
  (assert (re-matches #"/(?:.*[^/])?" context-path))
  (let [routes (routes context-path)
        path-for (path-for routes)
        opts (assoc opts :path-for path-for)]
    (-> (bidi-ring/make-handler routes (handlers opts))
        (wrap-path-for path-for)
        (wrap-not-found)
        (wrap-exception)
        (wrap-cors)
        (wrap-connection db-uri)
        (wrap-keyword-params)
        (wrap-params))))
