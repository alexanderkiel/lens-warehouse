(ns lens.app
  (:use plumbing.core)
  (:require [bidi.bidi :as bidi]
            [bidi.ring :as bidi-ring]
            [ring-hap.core :refer [wrap-hap]]
            [lens.route :refer [ContextPath routes]]
            [lens.handler :refer [handlers]]
            [lens.middleware.datomic :refer [wrap-connection]]
            [lens.middleware.cors :refer [wrap-cors]]
            [lens.middleware.log :refer [wrap-log]])
  (:import [java.net URI]))

(defn path-for [routes]
  (fn [handler & params]
    (URI/create (apply bidi/path-for routes handler params))))

(defn wrap-path-for [handler path-for]
  (fn [req] (handler (assoc req :path-for path-for))))

(defnk app [db-uri context-path :- ContextPath :as opts]
  (let [routes (routes context-path)
        path-for (path-for routes)
        opts (assoc opts :path-for path-for)]
    (-> (bidi-ring/make-handler routes (handlers opts))
        (wrap-path-for path-for)
        (wrap-connection db-uri)
        (wrap-hap {:up-href (path-for :service-document-handler)})
        (wrap-cors)
        (wrap-log))))
