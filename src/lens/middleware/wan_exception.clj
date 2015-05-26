(ns lens.middleware.wan-exception
  (:require [clj-stacktrace.repl :refer [pst-on]]))

(defn wrap-exception
  "Calls the handler in a try catch block returning a WAN error response."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Throwable e
        (pst-on *err* false e)
        {:status 500
         :body
         {:links {:up {:href "/"}}                          ;;TODO: use context path here
          :error (.getMessage e)}}))))
