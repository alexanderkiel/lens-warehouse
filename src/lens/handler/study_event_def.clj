(ns lens.handler.study-event-def
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource to-location]]
            [lens.handler.util :as hu]
            [lens.handler.study :as study]
            [lens.api :as api]
            [lens.reducers :as lr]
            [clojure.string :as str]
            [lens.util :as util]
            [schema.core :as s]))

(defn path [path-for study-event-def]
  (path-for :study-event-def-handler :eid (hu/entity-id study-event-def)))

(defn render-embedded [path-for entity]
  {:id (:study-event-def/id entity)
   :name (:study-event-def/name entity)
   :links
   {:self {:href (path path-for entity)}}})

(defn render-embedded-list [path-for entities]
  (r/map #(render-embedded path-for %) entities))

(def list-handler
  "Resource of all study-event-defs of a study."
  (resource
    (study/child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for params]]
      (let [page-num (hu/parse-page-num (:page-num params))
            filter (:filter params)
            study-events (if (str/blank? filter)
                           (->> (:study/study-event-defs study)
                                (sort-by :study-event-def/id))
                           (api/list-matching-study-event-defs study filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) study-events)))
            path #(-> (study/child-list-path :study-event-def path-for study %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up {:href (study/path path-for study)}
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))
         :queries
         {:lens/filter
          (hu/render-filter-query (study/child-list-path :study-event-def path-for study))}
         :forms
         {:lens/create-study-event-def
          (study/render-create-study-event-def-form path-for study)}
         :embedded
         {:lens/study-event-defs
          (->> (hu/paginate page-num study-events)
               (render-embedded-list path-for)
               (into []))}}))))

(defn- find-form-ref-path [path-for study-event-def]
  (path-for :find-form-ref-handler
            :study-id (-> study-event-def :study/_study-event-defs :study/id)
            :study-event-def-id (:study-event-def/id study-event-def)))

(defn- append-form-ref-path [path-for study-event-def]
  (path-for :append-form-ref-handler
            :study-id (-> study-event-def :study/_study-event-defs :study/id)
            :study-event-def-id (:study-event-def/id study-event-def)))

(def select-props (hu/select-props :study-event-def :name :desc))

(defnk render [study-event-def [:request path-for]]
  {:data
   (-> {:id (:study-event-def/id study-event-def)
        ;;TODO: alias
        :name (:study-event-def/name study-event-def)}
       (assoc-when :desc (:study-event-def/desc study-event-def)))

   :links
   {:up {:href (study/path path-for (:study/_study-event-defs study-event-def))}
    :self {:href (path path-for study-event-def)}
    :profile {:href (path-for :study-event-def-profile-handler)}}

   :queries
   {:lens/find-form-ref
    {:href (find-form-ref-path path-for study-event-def)
     :params {:form-id {:type s/Str}}}}

   :forms
   {:lens/append-form-ref
    {:href (append-form-ref-path path-for study-event-def)
     :params {:form-id {:type s/Str}}}}

   :ops #{:update :delete}})

(def schema {:name s/Str})

(def handler
  "Handler for GET, PUT and DELETE on a study-event-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the study-event-def taken from a database outside of
  the transaction. The update transaction is than tried with name and
  desc from that possibly old study-event-def as reference. The
  transaction only succeeds if the name and desc are still the same on
  the in-transaction study-event-def."
  (resource
    (hu/standard-entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      ((hu/entity-processable (assoc schema :id (s/eq (:study-event-def/id (api/find-entity db :study-event-def eid))))) ctx))

    :exists? (hu/exists? :study-event-def)

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (letk [[study-event-def] ctx]
          (hu/md5 (str (:media-type representation)
                       (study/path path-for (:study/_study-event-defs study-event-def))
                       (path path-for study-event-def)
                       (find-form-ref-path path-for study-event-def)
                       (append-form-ref-path path-for study-event-def)
                       (:name study-event-def)
                       (:desc study-event-def))))))

    :put!
    (fnk [conn study-event-def new-entity]
      (let [new-entity (util/prefix-namespace :study-event-def new-entity)]
        {:update-error (api/update-study-event-def conn study-event-def (select-props study-event-def)
                                                   (select-props new-entity))}))

    :delete!
    (fnk [conn study-event-def] (api/retract-entity conn (:db/id study-event-def)))

    :handle-ok render))

(def ^:private CreateParamSchema
  {:id util/NonBlankStr
   :name util/NonBlankStr
   (s/optional-key :desc) s/Str
   s/Any s/Any})

(def create-handler
  (resource
    (study/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn study [:request params]]
      (let [{:keys [id name]} params
            opts (->> (select-keys params [:desc])
                      (util/remove-nil-valued-entries)
                      (util/prefix-namespace :study-event-def))]
        (if-let [entity (api/create-study-event-def conn study id name opts)]
          {:entity entity}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [entity [:request path-for]] (path path-for entity))

    :handle-exception
    (study/duplicate-exception "The study event def exists already.")))

(defnk build-up-link [[:request path-for [:params study-id study-event-def-id]]]
  {:links {:up {:href (path-for :study-event-def-handler :study-id study-id
                                :study-event-def-id study-event-def-id)}}})

(defn redirect-resource-defaults []
  (assoc
    (hu/redirect-resource-defaults)

    :handle-unprocessable-entity
    (hu/error-handler "Unprocessable Entity" build-up-link)))
