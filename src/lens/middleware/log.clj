(ns lens.middleware.log
  (:require [plumbing.core :refer [letk]]
            [lens.logging :refer [warn error]]))

(defn wrap-log-errors [handler]
  (fn [req]
    (letk [[{status 200} :as resp] (handler req)]
      (cond
        (<= 500 status) (error {:type :server-error-response :response resp})
        (<= 400 status) (warn {:type :client-error-response :response resp}))
      resp)))
