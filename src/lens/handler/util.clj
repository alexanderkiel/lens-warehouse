(ns lens.handler.util
  (:use plumbing.core)
  (:require [cemerick.url :refer [url url-encode url-decode]]
            [liberator.core :as l]
            [liberator.representation :refer [Representation as-response]]
            [lens.util :as util]
            [pandect.algo.md5 :as md5]))

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
   (fn [data _]
     (handle-cache-control {:body data} opts))

   ;; Just respond with plain text here because the media type is negotiated
   ;; later in the decision graph.
   :handle-unauthorized (fn [{:keys [error]}] (or error "Not authorized."))

   :handle-not-modified nil})

(defnk entity-malformed
  "Standard malformed decision for single entity resources.

  Parsed entity will be placed under :new-entity in the context in case of
  success. Otherwise :errors will be placed."
  [request :as ctx]
  (if (or (not (l/=method :put ctx)) (l/header-exists? "if-match" ctx))
    (when (l/=method :put ctx)
      (if-let [params (:params request)]
        [false {:new-entity params}]
        {:error "Missing request body."}))
    {:error "Require conditional update."}))

(defn entity-processable [& params]
  (fn [ctx]
    (or (not (l/=method :put ctx))
        (every? identity (map #(get-in ctx [:new-entity %]) params)))))

(defn standard-entity-resource-defaults []
  (assoc
    (resource-defaults)

    :allowed-methods [:get :put :delete]

    :malformed? entity-malformed

    :processable? (entity-processable :name)

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
    (if (= :duplicate (util/error-type exception))
      (error path-for 409 msg)
      (throw exception))))

;; TODO: remove on release of https://github.com/clojure-liberator/liberator/pull/201
(defmethod l/to-location java.net.URI [uri] (l/to-location (str uri)))

(defn select-props [ns & props]
  (fn [m] (select-keys m (map #(keyword (name ns) (name %)) props))))

(def page-size 50)

(def paginate (partial util/paginate page-size))

(defn parse-page-num [s]
  (if (and s (re-matches #"[0-9]+" s))
    (util/parse-long s)
    1))

(defn assoc-filter [path filter]
  (if filter
    (str (assoc (url path) :query {:filter (url-encode filter)}))
    path))

(defn assoc-prev [m page-num path-fn]
  (assoc-when m :prev (when (< 1 page-num) (path-fn (dec page-num)))))

(defn assoc-next [m next-page? page-num path-fn]
  (assoc-when m :next (when next-page? (path-fn (inc page-num)))))

(defn render-embedded-count [self-href count]
  {:value count :links {:self {:href self-href}}})

(defn assoc-count
  "Assocs the count under :embedded :lens/count or a :len/count link with href
  if count is nil."
  [e count href]
  (if count
    (assoc-in e [:embedded :lens/count] (render-embedded-count href count))
    (assoc-in e [:links :lens/count :href] href)))

(def md5 md5/md5)

(defn render-filter-query [uri]
  {:href uri
   :title "Filter"
   :params
   {:filter
    {:type 'Str
     :desc "Search query which allows Lucene syntax."}}})
