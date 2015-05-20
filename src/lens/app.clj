(ns lens.app
  (:require [lens.routes :refer [routes]]
            [lens.middleware.datomic :refer [wrap-connection]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [lens.middleware.wan-exception :refer [wrap-exception]]
            [lens.middleware.cors :refer [wrap-cors]]
            [ring.middleware.format :refer [wrap-restful-format]]))

(defn app [db-uri version]
  (-> (routes version)
      (wrap-exception)
      (wrap-restful-format)
      (wrap-cors)
      (wrap-connection db-uri)
      (wrap-keyword-params)
      (wrap-params)))
