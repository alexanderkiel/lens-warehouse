(ns lens.handler.item-def
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

(defn path [path-for item-def]
  (path-for :item-def-handler :eid (hu/entity-id item-def)))

(defn link [path-for item-def]
  {:href (path path-for item-def)
   :label (:item-def/name item-def)})

(defn render-embedded [path-for timeout item-def]
  (-> {:id (:item-def/id item-def)
       :name (:item-def/name item-def)
       :data-type (keyword (name (:item-def/data-type item-def)))
       :links
       (-> {:self (link path-for item-def)}
           #_(assoc-code-list-link item))}
      (assoc-when :desc (:item-def/desc item-def))
      (assoc-when :question (:item-def/question item-def))
      #_(assoc-count
        (util/try-until timeout (api/num-item-subjects item))
        (path-for :item-def-count-handler :id (:item/id item)))))

(defn render-embedded-list [path-for timeout item-defs]
  (r/map #(render-embedded path-for timeout %) item-defs))

(def list-handler
  "Resource of all item-defs of a study."
  (resource
    (study/child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for [:params page-num {filter nil}]]]
      (let [items (if (str/blank? filter)
                    (->> (:study/item-defs study)
                         (sort-by :item-def/id))
                    (api/list-matching-item-defs study filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) items)))
            path #(-> (study/child-list-path :item-def path-for study %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up (study/link path-for study)
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))

         :queries
         {:lens/filter
          (hu/render-filter-query (study/child-list-path :item-def path-for study))}

         :forms
         {:lens/create-item-def
          (study/render-create-item-def-form path-for study)}

         :embedded
         {:lens/item-defs
          (->> (hu/paginate page-num items)
               (render-embedded-list path-for (timeout 100))
               (into []))}}))))

(def select-props (hu/select-props :item-def :name :data-type :desc :question))

(def prefix-data-type (partial util/prefix-namespace :data-type))

(defnk render [item-def [:request path-for]]
  {:data
   (-> {:id (:item-def/id item-def)
        ;;TODO: alias
        :name (:item-def/name item-def)
        :data-type (keyword (name (:item-def/data-type item-def)))}
       (assoc-when :desc (:item-def/desc item-def))
       (assoc-when :question (:item-def/question item-def)))

   :links
   {:up (study/link path-for (:study/_item-defs item-def))
    :self (link path-for item-def)
    :profile {:href (path-for :item-def-profile-handler)}}

   :ops #{:update :delete}})

(def schema {:name s/Str
             :data-type study/item-def-data-type-schema
             (s/optional-key :length) s/Int
             (s/optional-key :significant-digits) s/Int
             (s/optional-key :origin) s/Str
             (s/optional-key :comment) s/Str
             (s/optional-key :desc) s/Str
             (s/optional-key :question) s/Str})

(def handler
  "Handler for GET, PUT and DELETE on an item-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the item-def taken from a database outside of the
  transaction. The update transaction is than tried with name and desc
  from that possibly old item as reference. The transaction only succeeds if the
  name and desc are still the same on the in-transaction item-def."
  (resource
    (hu/entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      (let [item-def (api/find-entity db :item-def (hu/to-eid db eid))]
        ((hu/entity-processable (assoc schema :id (s/eq (:item-def/id item-def)))) ctx)))

    :exists? (hu/exists? :item-def)

    :etag
    (hu/etag #(-> % :item-def :item-def/name)
             #(-> % :item-def :item-def/data-type)
             #(-> % :item-def :item-def/desc)
             #(-> % :item-def :item-def/question)
             1)

    :put!
    (fnk [conn item-def new-entity]
      (let [new-entity (->> (update new-entity :data-type prefix-data-type)
                            (util/prefix-namespace :item-def))]
        {:update-error (api/update conn item-def (select-props item-def)
                                   (select-props new-entity))}))

    :delete!
    (fnk [conn item-def] (api/retract-entity conn (:db/id item-def)))

    :handle-ok render))

(defn item-def-count-handler [path-for]
  (resource
    (hu/resource-defaults)

    :exists? (hu/exists? :item-def)

    :handle-ok
    (fnk [item-def]
      {:value (api/num-item-subjects item-def)
       :links
       {:up {:href (path-for :item-def-handler :id (:item/id item-def))}
        :self {:href (path-for :item-def-count-handler :id (:item/id item-def))}}})

    :handle-not-found
    (hu/error-body path-for "Item not found.")))

(def ^:private CreateParamSchema
  {:id util/NonBlankStr
   :data-type study/item-def-data-type-schema
   :name util/NonBlankStr
   (s/optional-key :desc) s/Str
   s/Any s/Any})

(def create-handler
  (resource
    (study/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn study [:request params]]
      (let [{:keys [id name data-type]} params
            data-type (keyword "data-type" (clojure.core/name data-type))
            opts (->> (select-keys params [:desc :question :length])
                      (util/prefix-namespace :item-def))]
        (if-let [entity (api/create-item-def conn study id name data-type opts)]
          {:entity entity}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [entity [:request path-for]] (path path-for entity))

    :handle-exception
    (study/duplicate-exception "The item def exists already.")))
