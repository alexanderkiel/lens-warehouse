(ns lens.handler.util
  (:use plumbing.core)
  (:require [clojure.data.json :as json]
            [cognitect.transit :as transit]
            [liberator.core :as l]
            [liberator.representation :refer [Representation as-response]]))

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

(defn parse-body [content-type body]
  (condp = content-type
    "application/json"
    (json/read-str (slurp body) :key-fn keyword)
    "application/transit+json"
    (transit/read (transit/reader body :json))))

(defnk entity-malformed
  "Standard malformed decision for single entity resources.

  Parsed entity will be placed under :new-entity in context in case of success.
  Otherwise :errors will be placed."
  [request :as ctx]
  (if (or (l/=method :get ctx) (l/header-exists? "if-match" ctx))
    (when (= :put (:request-method request))
      (if-let [body (:body request)]
        (let [content-type (get-in request [:headers "content-type"])]
          (try
            [false {:new-entity (parse-body content-type body)}]
            (catch Exception _ {:error "Invalid request body."})))
        {:error "Missing request body."}))
    {:error "Require conditional update."}))

(defn entity-processable [ctx] (or (l/=method :get ctx) (-> ctx :new-entity :name)))

(defn entity-exists [key accessor]
  (fnk [db [:request [:params id]]]
    (when-let [entity (accessor db id)]
      {key entity})))

(defn standard-entity-resource-defaults [path-for]
  (merge
    (resource-defaults)

    {:allowed-methods [:get :put]

     :malformed? entity-malformed

     :processable? entity-processable

     :can-put-to-missing? false

     :new? false

     :handle-no-content
     (fnk [update-error]
       (condp = update-error
         :not-found (error path-for 404 "Not found.")
         :conflict (error path-for 409 "Conflict")
         nil))

     :handle-malformed
     (fnk [error] error)

     :handle-not-found
     (error-body path-for "Not found.")}))
