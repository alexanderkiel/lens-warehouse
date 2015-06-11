(ns lens.middleware.cors)

(defn assoc-header [response]
  (-> (assoc-in response [:headers "Access-Control-Allow-Origin"] "*")
      (assoc-in [:headers "Access-Control-Expose-Headers"] "ETag, Location")))

(defn wrap-cors
  "Adds an Access-Control-Allow-Origin header with the value * to responses."
  [handler]
  (fn [request]
    (if (= :options (:request-method request))
      {:status 204
       :headers {"Access-Control-Allow-Origin" "*"
                 "Access-Control-Allow-Methods" "GET, POST, PUT, DELETE"
                 "Access-Control-Allow-Headers"
                 "Authorization, Accept, If-Match, X-Lens-Snapshot, Content-Type"
                 "Access-Control-Max-Age" "3600"}}
      (-> (handler request)
          (assoc-header)))))
