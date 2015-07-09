(ns lens.handler.util
  (:use plumbing.core)
  (:require [cemerick.url :refer [url url-encode url-decode]]
            [liberator.core :as l :refer [resource]]
            [liberator.representation :refer [Representation as-response]]
            [lens.util :as util]
            [pandect.algo.md5 :as md5]
            [schema.core :as s]
            [lens.api :as api]
            [shortid.core :as sid])
  (:refer-clojure :exclude [error-handler]))

(defn error-body
  ([path-for msg]
   (error-body path-for msg nil))
  ([path-for msg more]
   (merge
     {:data {:message msg}
      :links {:up {:href (path-for :service-document-handler)}}}
     more)))

(defn ring-error [path-for status msg]
  {:status status
   :body (error-body path-for msg)})

(defrecord StatusResponse [status response]
  Representation
  (as-response [_ context]
    (assoc (as-response response context) :status status)))

(defn error
  ([path-for status msg]
   (error path-for status msg nil))
  ([path-for status msg more]
   (->StatusResponse status (error-body path-for msg more))))

(defn handle-cache-control [resp opts]
  (if-let [cache-control (:cache-control opts)]
    (assoc-in resp [:headers "cache-control"] cache-control)
    resp))

(extend-protocol Representation
  clojure.lang.MapEquivalence
  (as-response [this _] {:body this}))

(defn error-handler
  ([msg]
   (error-handler msg nil))
  ([msg more]
   (fnk [[:request path-for] :as ctx]
     (error-body path-for (or (:error ctx) msg)
                 (if (fn? more) (more ctx) more)))))

(defn resource-defaults [& {:as opts}]
  {:available-media-types ["application/transit+json"]

   :service-available?
   (fnk [request]
     (let [conn (:conn request)
           db (:db request)]
       (when (or conn db)
         {:conn conn :db db})))

   :as-response
   (fn [data ctx]
     (handle-cache-control (as-response data ctx) opts))

   ;; Just respond with plain text here because the media type is negotiated
   ;; later in the decision graph.
   :handle-unauthorized (fn [{:keys [error]}] (or error "Not authorized."))
   :handle-malformed (error-handler "Malformed")
   :handle-unprocessable-entity (error-handler "Unprocessable Entity")
   :handle-precondition-failed (error-handler "Precondition Failed")
   :handle-not-found (error-handler "Not Found")

   :handle-not-modified nil})

(defnk entity-malformed
  "Standard malformed decision for single entity resources.

  Parsed entity will be placed under :new-entity in the context in case of
  success. Otherwise :error will be placed."
  [request :as ctx]
  (if (or (not (l/=method :put ctx)) (l/header-exists? "if-match" ctx))
    (when (l/=method :put ctx)
      (if-let [body (:body request)]
        [false {:new-entity (:data body)}]
        {:error "Missing request body."}))
    {:error "Require conditional update."}))

(defn validate [schema x]
  (if-let [error (s/check schema x)]
    [false {:error (str "Unprocessable Entity: " (pr-str error))}]
    true))

(defn validate-params [schema]
  (fnk [[:request params]]
    (validate schema params)))

(defn entity-processable [schema]
  (fn [ctx]
    (or (not (l/=method :put ctx))
        (validate schema (:new-entity ctx)))))

(defn standard-entity-resource-defaults []
  (assoc
    (resource-defaults)

    :allowed-methods [:get :put :delete]

    :malformed? entity-malformed

    :can-put-to-missing? false

    :new? false

    :handle-no-content
    (fnk [[:request path-for] :as ctx]
      (condp = (:update-error ctx)
        :not-found (error path-for 404 "Not Found")
        :conflict (error path-for 409 "Conflict")
        nil))))

(defn redirect-resource-defaults []
  (assoc
    (resource-defaults)

    :exists? false
    :existed? true
    :moved-permanently? true))

(defn create-resource-defaults []
  (assoc
    (resource-defaults)

    :allowed-methods [:post]

    :can-post-to-missing? false))

(defn duplicate-exception
  ([msg]
   (duplicate-exception msg nil))
  ([msg more]
   (fnk [exception [:request path-for] :as ctx]
     (if (= :duplicate (util/error-type exception))
       (error path-for 409 msg (if (fn? more) (more ctx) more))
       (throw exception)))))

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
    {:type s/Str
     :desc "Search query which allows Lucene syntax."}}})

(defn profile-handler [key schema]
  (let [name (keyword (str (name key) "-profile-handler"))]
    (resource
      (resource-defaults :cache-control "max-age=3600")

      ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
      :etag
      (fnk [representation {status 200} [:request path-for]]
        (when (= 200 status)
          (md5 (str (:media-type representation)
                    (path-for :service-document-handler)
                    (path-for name)))))

      :handle-ok
      (fnk [[:request path-for]]
        {:data
         {:schema schema}
         :links
         {:up {:href (path-for :service-document-handler)}
          :self {:href (path-for name)}}}))))

(defn exists? [type]
  (fnk [db [:request [:params eid]]]
    (when-let [entity (api/find-entity db type eid)]
      {type entity})))

(defn entity-id [entity]
  (sid/int-to-base62 (:db/id entity)))
