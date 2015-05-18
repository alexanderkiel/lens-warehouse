(ns lens.handler.util
  (:use plumbing.core))

(defn decode-etag [etag]
  (subs etag 1 (dec (count etag))))

(defn error-body [path-for msg]
  {:links {:up {:href (path-for :service-document-handler)}}
   :error msg})

(defn error [path-for status msg]
  {:status status
   :body (error-body path-for msg)})

(def resource-defaults
  {:available-media-types ["application/json" "application/transit+json"
                           "application/edn"]

   :service-available?
   (fnk [request]
     (let [conn (:conn request)
           db (:db request)]
       (when (or conn db)
         {:conn conn :db db})))

   ;; Just respond with plain text here because the media type is negotiated
   ;; later in the decision graph.
   :handle-unauthorized (fn [{:keys [error]}] (or error "Not authorized."))})
