(ns lens.handler.util
  (:use plumbing.core)
  (:require [liberator.core :as l]
            [liberator.representation :refer [Representation as-response]]
            [lens.util :as util]
            [clojure.tools.logging :as log]))

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
  {:available-media-types ["application/transit+json"]

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

(defnk entity-malformed
  "Standard malformed decision for single entity resources.

  Parsed entity will be placed under :new-entity in context in case of success.
  Otherwise :errors will be placed."
  [request :as ctx]
  (if (or (l/=method :get ctx) (l/header-exists? "if-match" ctx))
    (when (= :put (:request-method request))
      (if-let [params (:params request)]
        [false {:new-entity params}]
        {:error "Missing request body."}))
    {:error "Require conditional update."}))

(defn entity-processable [ctx] (or (l/=method :get ctx) (-> ctx :new-entity :name)))

(defn standard-entity-resource-defaults []
  (assoc
    (resource-defaults)

    :allowed-methods [:get :put]

    :malformed? entity-malformed

    :processable? entity-processable

    :can-put-to-missing? false

    :new? false

    :handle-no-content
    (fnk [[:request path-for] :as ctx]
      (condp = (:update-error ctx)
        :not-found (error path-for 404 "Not found.")
        :conflict (error path-for 409 "Conflict")
        nil))

    :handle-malformed
    (fnk [error] error)

    :handle-not-found
    (fnk [[:request path-for]] (error-body path-for "Not found."))))

(defn standard-redirect-resource-defaults []
  (assoc
    (resource-defaults)

    :exists? false
    :existed? true
    :moved-permanently? true))

(defn sub-study-redirect-resource-defaults []
  (assoc
    (standard-redirect-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params)))))

(defn standard-create-resource-defaults []
  (assoc
    (resource-defaults)

    :allowed-methods [:post]

    :can-post-to-missing? false

    :handle-unprocessable-entity
    (fnk [[:request path-for] :as ctx]
      (error-body path-for (or (:error ctx) "Unprocessable Entity")))))

(defn duplicate-exception [msg]
  (fnk [exception [:request path-for]]
    (if (= ::duplicate (util/error-type exception))
      (error path-for 409 msg)
      (throw exception))))

;; TODO: remove on release of https://github.com/clojure-liberator/liberator/pull/201
(defmethod l/to-location java.net.URI [uri] (l/to-location (str uri)))

(defn prefix-namespace [ns m]
  (if (map? m)
    (map-keys #(prefix-namespace ns %) m)
    (keyword (name ns) (name m))))

(defn select-props [& props]
  (fn [m] (select-keys m props)))
