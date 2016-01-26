(ns lens.app
  (:use plumbing.core)
  (:require [bidi.bidi :as bidi]
            [bidi.ring :as bidi-ring]
            [ring-hap.core :refer [wrap-hap]]
            [schema.core :refer [Str]]
            [lens.route :refer [ContextPath routes]]
            [lens.search.api :as search-api]
            [lens.handler :refer [handlers]]
            [lens.middleware.datomic :refer [wrap-connection]]
            [lens.middleware.cors :refer [wrap-cors]]
            [lens.middleware.log :refer [wrap-log-errors]])
  (:import [java.net URI]))

(defn path-for [routes]
  (fn [handler & params]
    (URI/create (apply bidi/path-for routes handler params))))

(defn wrap-path-for [handler path-for]
  (fn [req] (handler (assoc req :path-for path-for))))

(defn wrap-search-conn [handler search-conn]
  (fn [req] (handler (assoc req :search-conn search-conn))))

(defnk app
  "Whole app Ring handler."
  [db-uri :- Str search-conn :- search-api/Conn
   context-path :- ContextPath :as opts]
  (let [routes (routes context-path)
        path-for (path-for routes)
        opts (assoc opts :path-for path-for)]
    (-> (bidi-ring/make-handler routes (handlers opts))
        (wrap-path-for path-for)
        (wrap-search-conn search-conn)
        (wrap-connection db-uri)
        (wrap-log-errors)
        (wrap-hap {:up-href (path-for :service-document-handler)})
        (wrap-cors))))
