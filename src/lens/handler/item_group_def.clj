(ns lens.handler.item-group-def
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource to-location]]
            [lens.handler.util :as hu]
            [lens.handler.study :refer :all]
            [lens.api :as api]
            [lens.reducers :as lr]
            [clojure.string :as str]
            [lens.util :as util]
            [schema.core :as s]))

(defn render-embedded [path-for timeout def]
  (-> {:id (:item-group-def/id def)
       ;;TODO: alias
       :name (:item-group-def/name def)
       :links
       {:self
        {:href (child-path :item-group-def path-for def)}}}
      #_(assoc-count
        (util/try-until timeout (api/num-item-group-subjects item-group-def))
        (path-for :item-group-count-handler :id (:item-group/id item-group-def)))))

(defn render-embedded-list [path-for timeout defs]
  (r/map #(render-embedded path-for timeout %) defs))

(def list-handler
  "Resource of all item-group-defs of a study."
  (resource
    (study-child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for params]]
      (let [page-num (hu/parse-page-num (:page-num params))
            filter (:filter params)
            item-groups (if (str/blank? filter)
                          (->> (:study/item-group-defs study)
                               (sort-by :item-group-def/id))
                          (api/list-matching-item-group-defs study filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) item-groups)))
            path #(-> (child-list-path :item-group-def path-for study %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up {:href (study-path path-for study)}
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))

         :queries
         {:lens/filter
          (hu/render-filter-query (child-list-path :item-group-def path-for study))}

         :forms
         {:lens/create-item-group-def
          (render-item-group-create-form path-for study)}

         :embedded
         {:lens/item-group-defs
          (->> (hu/paginate page-num item-groups)
               (render-embedded-list path-for (timeout 100))
               (into []))}}))))

(def find-handler
  (resource
    (hu/sub-study-redirect-resource-defaults)

    :location
    (fnk [[:request path-for [:params study-id id]]]
      (child-path :item-group-def path-for study-id id))))

(def exists? (exists-study-child? :item-group-def))

(def select-props (hu/select-props :item-group-def :name :desc))

(defnk render [def [:request path-for]]
  (-> {:id (:item-group-def/id def)
       ;;TODO: alias
       :name (:item-group-def/name def)}
      (assoc-when :desc (:item-group-def/desc def))
      (assoc
        :links
        {:up (study-link path-for (:study/_item-group-defs def))
         :self {:href (child-path :item-group-def path-for def)}}

        :ops [:update :delete])))

(def ^:private schema {:name s/Str})

(def handler
  "Handler for GET and PUT on an item-group-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the item-group-def taken from a database outside of
  the transaction. The update transaction is than tried with name and
  desc from that possibly old item-group-def as reference. The
  transaction only succeeds if the name and desc are still the same on
  the in-transaction item-group-def."
  (resource
    (hu/standard-entity-resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists?)))

    :processable? (hu/entity-processable schema)

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (letk [[def] ctx]
          (hu/md5 (str (:media-type representation)
                       (study-path path-for (:study/_item-group-defs def))
                       (child-path :item-group-def path-for def)
                       (:name def)
                       (:desc def))))))

    :put!
    (fnk [conn def new-entity]
      (let [new-entity (util/prefix-namespace :item-group-def new-entity)]
        {:update-error (api/update-item-group-def conn def (select-props def)
                                                  (select-props new-entity))}))

    :delete!
    (fnk [conn def] (api/retract-entity conn (:db/id def)))

    :handle-ok render))

(defn item-group-def-count-handler [path-for]
  (resource
    (hu/resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists?)))

    :handle-ok
    (fnk [item-group-def]
      (let [id (:item-group/id item-group-def)]
        {:value (api/num-item-group-subjects item-group-def)
         :links
         {:up {:href (child-path :item-group-def path-for item-group-def)}
          :self {:href (path-for :item-group-count-handler :id id)}}}))

    :handle-not-found
    (hu/error-body path-for "Item group not found.")))

(def create-handler
  (resource
    (hu/standard-create-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params) (:name params)))

    :exists? exists-study?

    :post!
    (fnk [conn study [:request params]]
      (let [{:keys [id name]} params
            opts (->> (select-keys params [:desc])
                      (util/remove-nil-valued-entries)
                      (util/prefix-namespace :item-group-def))]
        (if-let [def (api/create-item-group-def conn study id name opts)]
          {:def def}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [def [:request path-for]] (child-path :item-group-def path-for def))

    :handle-exception
    (hu/duplicate-exception "The item group def exists already.")))
