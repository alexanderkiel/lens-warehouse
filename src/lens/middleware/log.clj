(ns lens.middleware.log
  (:require [lens.logging :refer [info]]
            [lens.util :as util]))

(defn- filter-response [{:keys [status] :as resp}]
  (select-keys resp (cond-> [:status :headers] (<= 400 status) (conj :body))))

(defn wrap-log [handler]
  (fn [req]
    (let [start (System/nanoTime)
          resp (handler req)]
      (info {:request (select-keys req [:remote-addr :request-method
                                        :uri :query-string :headers])
             :response (filter-response resp)
             :took (util/duration start)})
      resp)))
