(ns lens.handler.util
  (:use plumbing.core)
  (:require [liberator.representation :refer [Representation as-response]]))

(defn error-body [path-for msg]
  {:links {:up {:href (path-for :service-document-handler)}}
   :error msg})

(defn ring-error [path-for status msg]
  {:status status
   :body (error-body path-for msg)})

(defrecord StatusResponse [status response]
  Representation
  (as-response [_ context]
    (assoc (as-response response context) :status status)))

(defn error [path-for status msg]
  (->StatusResponse status (error-body path-for msg)))

(defn handle-cache-control [resp opts]
  (if-let [cache-control (:cache-control opts)]
    (assoc-in resp [:headers "cache-control"] cache-control)
    resp))

(defn resource-defaults [& {:as opts}]
  {:available-media-types ["application/json" "application/transit+json"
                           "application/edn"]

   :service-available?
   (fnk [request]
     (let [conn (:conn request)
           db (:db request)]
       (when (or conn db)
         {:conn conn :db db})))

   :as-response
   (fn [d ctx]
     (-> (as-response d ctx)
         (handle-cache-control opts)))

   ;; Just respond with plain text here because the media type is negotiated
   ;; later in the decision graph.
   :handle-unauthorized (fn [{:keys [error]}] (or error "Not authorized."))

   :handle-not-modified nil})
