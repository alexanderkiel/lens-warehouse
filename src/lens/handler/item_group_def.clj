(ns lens.handler.item-group-def
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource]]
            [lens.handler.util :as hu]
            [lens.handler.study :as study]
            [lens.api :as api]
            [lens.reducers :as lr]
            [clojure.string :as str]
            [lens.util :as util]
            [schema.core :as s]))

(defn path [path-for item-group-def]
  (path-for :item-group-def-handler :eid (hu/entity-id item-group-def)))

(defn link [path-for item-group-def]
  {:href (path path-for item-group-def)
   :label (str "Item Group Def " (:item-group-def/name item-group-def))})

(defn render-embedded [path-for timeout item-group-def]
  (-> {:id (:item-group-def/id item-group-def)
       ;;TODO: alias
       :name (:item-group-def/name item-group-def)
       :links
       {:self
        (link path-for item-group-def)}}
      #_(assoc-count
        (util/try-until timeout (api/num-item-group-subjects item-group-def))
        (path-for :item-group-count-handler :id (:item-group/id item-group-def)))))

(defn render-embedded-list [path-for timeout item-group-defs]
  (r/map #(render-embedded path-for timeout %) item-group-defs))

(def list-handler
  "Resource of all item-group-defs of a study."
  (resource
    (study/child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for [:params page-num {filter nil}]]]
      (let [item-groups (if (str/blank? filter)
                          (->> (:study/item-group-defs study)
                               (sort-by :item-group-def/id))
                          (api/list-matching-item-group-defs study filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) item-groups)))
            path #(-> (study/child-list-path :item-group-def path-for study %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up (study/link path-for study)
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))

         :queries
         {:lens/filter
          (hu/render-filter-query
            (study/child-list-path :item-group-def path-for study))}

         :forms
         {:lens/create-item-group-def
          (study/render-create-item-group-create-form path-for study)}

         :embedded
         {:lens/item-group-defs
          (->> (hu/paginate page-num item-groups)
               (render-embedded-list path-for (timeout 100))
               (into []))}}))))

(defn- find-item-ref-path [path-for form-def]
  (path-for :find-item-ref-handler :eid (hu/entity-id form-def)))

(defn- item-refs-path [path-for form-def]
  (path-for :item-refs-handler :eid (hu/entity-id form-def) :page-num 1))

(defn- create-item-ref-path [path-for form-def]
  (path-for :create-item-ref-handler :eid (hu/entity-id form-def)))

(defn create-item-ref-form [path-for form-def]
  {:href (create-item-ref-path path-for form-def)
   :params {:form-id {:type s/Str}}})

(def select-props (hu/select-props :item-group-def :name :desc))

(defnk render [item-group-def [:request path-for]]
  {:data
   (-> {:id (:item-group-def/id item-group-def)
        ;;TODO: alias
        :name (:item-group-def/name item-group-def)}
       (assoc-when :desc (:item-group-def/desc item-group-def)))

   :links
   {:up (study/link path-for (:study/_item-group-defs item-group-def))
    :self (link path-for item-group-def)
    :profile {:href (path-for :item-group-def-profile-handler)}
    :lens/item-refs {:href (item-refs-path path-for item-group-def)}}

   :queries
   {:lens/find-item-ref
    {:href (find-item-ref-path path-for item-group-def)
     :params {:item-id {:type s/Str}}}}

   :forms
   {:lens/create-item-ref
    (create-item-ref-form path-for item-group-def)}

   :ops #{:update :delete}})

(def schema {:name s/Str (s/optional-key :desc) s/Str})

(def handler
  "Handler for GET, PUT and DELETE on an item-group-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the item-group-def taken from a database outside of
  the transaction. The update transaction is than tried with name and
  desc from that possibly old item-group-def as reference. The
  transaction only succeeds if the name and desc are still the same on
  the in-transaction item-group-def."
  (resource
    (hu/entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      (let [item-group-def (api/find-entity db :item-group-def (hu/to-eid db eid))]
        ((hu/entity-processable (assoc schema :id (s/eq (:item-group-def/id item-group-def)))) ctx)))

    :exists? (hu/exists? :item-group-def)

    :etag
    (hu/etag #(-> % :item-group-def :item-group-def/name)
             #(-> % :item-group-def :item-group-def/desc)
             1)

    :put!
    (fnk [conn item-group-def new-entity]
      (let [new-entity (util/prefix-namespace :item-group-def new-entity)]
        {:update-error (api/update-item-group-def conn item-group-def (select-props item-group-def)
                                                  (select-props new-entity))}))

    :delete!
    (fnk [conn item-group-def] (api/retract-entity conn (:db/id item-group-def)))

    :handle-ok render))

(defn item-group-def-count-handler [path-for]
  (resource
    (hu/resource-defaults)

    :exists? (hu/exists? :item-group-def)

    :handle-ok
    (fnk [entity]
      (let [id (:item-group/id entity)]
        {:value (api/num-item-group-subjects entity)
         :links
         {:up {:href (path path-for entity)}
          :self {:href (path-for :item-group-count-handler :id id)}}}))

    :handle-not-found
    (hu/error-body path-for "Item group not found.")))

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
                      (util/prefix-namespace :item-group-def))]
        (if-let [entity (api/create-item-group-def conn study id name opts)]
          {:entity entity}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [entity [:request path-for]] (path path-for entity))

    :handle-exception
    (study/duplicate-exception "The item group def exists already.")))

;; ---- For Childs ------------------------------------------------------------

(defnk build-up-link [[:request path-for [:params eid]]]
  {:links {:up {:href (path-for :item-group-def-handler :eid eid)}}})

(def ^:private ChildListParamSchema
  {:eid util/Base62EntityId
   :page-num util/PosInt
   s/Any s/Any})

(defn child-list-resource-defaults []
  (assoc
    (hu/resource-defaults)

    :processable? (hu/coerce-params ChildListParamSchema)

    :exists? (hu/exists? :item-group-def)))

(defn redirect-resource-defaults []
  (assoc
    (hu/redirect-resource-defaults)

    :handle-unprocessable-entity
    (hu/error-handler "Unprocessable Entity" build-up-link)))

(defn create-resource-defaults []
  (assoc
    (hu/create-resource-defaults)

    :exists? (hu/exists? :item-group-def)))
