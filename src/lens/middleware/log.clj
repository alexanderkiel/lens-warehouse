(ns lens.middleware.log
  (:require [lens.logging :refer [info]]
            [lens.util :as util]))

(defn wrap-log [handler]
  (fn [req]
    (let [start (System/nanoTime)
          resp (handler req)]
      (info {:request (select-keys req [:remote-addr :request-method
                                        :uri :query-string :headers])
             :response (select-keys resp [:status :headers])
             :took (util/duration start)})
      resp)))
