(ns lens.middleware.log
  (:require [clojure.tools.logging :as log]))

(defn- duration [start end]
  (Math/round (/ (double (- end start)) 1000000)))

(defn wrap-log [handler]
  (fn [req]
    (let [start (System/nanoTime)
          resp (handler req)
          end (System/nanoTime)]
      (log/info (pr-str {:request (select-keys req [:remote-addr :request-method
                                                    :uri :query-string :headers])
                         :response (select-keys resp [:status :headers])
                         :duration (str (duration start end) " ms")}))
      resp)))
