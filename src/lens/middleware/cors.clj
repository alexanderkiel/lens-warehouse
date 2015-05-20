(ns lens.middleware.cors)

(defn assoc-header [response]
  (assoc-in response [:headers "Access-Control-Allow-Origin"] "*"))

(defn wrap-cors
  "Adds an Access-Control-Allow-Origin header with the value * to responses."
  [handler]
  (fn [request]
    (-> request
        (handler)
        (assoc-header))))
