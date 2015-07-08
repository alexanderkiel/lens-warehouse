(ns lens.handler.item-def
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

(defn render-embedded [path-for timeout def]
  (-> {:id (:item-def/id def)
       :name (:item-def/name def)
       :data-type (keyword (name (:item-def/data-type def)))
       :links
       (-> {:self {:href (study/child-path :item-def path-for def)}}
           #_(assoc-code-list-link item))}
      (assoc-when :desc (:item-def/desc def))
      (assoc-when :question (:item-def/question def))
      #_(assoc-count
        (util/try-until timeout (api/num-item-subjects item))
        (path-for :item-def-count-handler :id (:item/id item)))))

(defn render-embedded-list [path-for timeout def]
  (r/map #(render-embedded path-for timeout %) def))

(def list-handler
  "Resource of all item-defs of a study."
  (resource
    (study/study-child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for params]]
      (let [page-num (hu/parse-page-num (:page-num params))
            filter (:filter params)
            items (if (str/blank? filter)
                    (->> (:study/item-defs study)
                         (sort-by :item-def/id))
                    (api/list-matching-item-defs study filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) items)))
            path #(-> (study/child-list-path :item-def path-for study %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up {:href (study/path path-for study)}
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

(def find-handler
  (resource
    (study/redirect-resource-defaults)

    :location
    (fnk [[:request path-for [:params study-id id]]]
      (study/child-path :item-def path-for study-id id))))

(def exists? (study/exists-study-child? :item-def))

(def select-props (hu/select-props :item-def :name :data-type :desc :question))

(def prefix-data-type (partial util/prefix-namespace :data-type))

(defnk render [def [:request path-for]]
  {:data
   (-> {:id (:item-def/id def)
        ;;TODO: alias
        :name (:item-def/name def)
        :data-type (keyword (name (:item-def/data-type def)))}
       (assoc-when :desc (:item-def/desc def))
       (assoc-when :question (:item-def/question def)))

   :links
   {:up (study/study-link path-for (:study/_item-defs def))
    :self {:href (study/child-path :item-def path-for def)}
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
  "Handler for GET and PUT on an item-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the item-def taken from a database outside of the
  transaction. The update transaction is than tried with name and desc
  from that possibly old item as reference. The transaction only succeeds if the
  name and desc are still the same on the in-transaction item-def."
  (resource
    (hu/standard-entity-resource-defaults)

    :processable?
    (fnk [[:request [:params item-def-id]] :as ctx]
      ((hu/entity-processable (assoc schema :id (s/eq item-def-id))) ctx))

    :exists? (fn [ctx] (some-> (study/exists? ctx) (exists?)))

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (letk [[def] ctx]
          (hu/md5 (str (:media-type representation)
                       (study/path path-for (:study/_item-defs def))
                       (study/child-path :item-def path-for def)
                       (:item-def/name def)
                       (:item-def/data-type def)
                       (:item-def/desc def)
                       (:item-def/question def))))))

    :put!
    (fnk [conn def new-entity]
      (let [new-entity (->> (update new-entity :data-type prefix-data-type)
                            (util/prefix-namespace :item-def))]
        {:update-error (api/update conn def (select-props def)
                                   (select-props new-entity))}))

    :delete!
    (fnk [conn def] (api/retract-entity conn (:db/id def)))

    :handle-ok render))

(defn item-def-count-handler [path-for]
  (resource
    (hu/resource-defaults)

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item (api/find-study-child db :item-def id)]
        {:item item}))

    :handle-ok
    (fnk [item]
      {:value (api/num-item-subjects item)
       :links
       {:up {:href (path-for :item-def-handler :id (:item/id item))}
        :self {:href (path-for :item-def-count-handler :id (:item/id item))}}})

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
        (if-let [def (api/create-item-def conn study id name data-type opts)]
          {:def def}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [def [:request path-for]] (study/child-path :item-def path-for def))

    :handle-exception
    (study/duplicate-exception "The item def exists already.")))
