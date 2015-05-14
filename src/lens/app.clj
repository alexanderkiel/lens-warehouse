(ns lens.app
  (:use plumbing.core)
  (:require [lens.routes :refer [routes]]
            [lens.middleware.datomic :refer [wrap-connection]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [lens.middleware.wan-exception :refer [wrap-exception]]
            [lens.middleware.cors :refer [wrap-cors]]
            [ring.middleware.format :refer [wrap-restful-format]]))

(defnk app [db-uri context-path version]
  (-> (routes version)
      (wrap-connection db-uri)
      (wrap-exception)
      (wrap-restful-format)
      (wrap-keyword-params)
      (wrap-params)
      (wrap-cors)))
