(ns lens.handler.item-ref
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [schema.core :as s]
            [lens.handler.util :as hu]
            [lens.handler.item-group-def :as item-group-def]
            [lens.api :as api]
            [lens.util :as util]
            [lens.handler.item-def :as item-def]
            [lens.reducers :as lr]
            [clojure.core.reducers :as r]))

(defn- path
  ([path-for item-ref]
   (path-for :item-ref-handler :eid (hu/entity-id item-ref))))

(defn link [path-for item-ref]
  {:href (path path-for item-ref)
   :label (-> item-ref :item-ref/item :item-def/name)})

(defn render-embedded [path-for item-ref]
  {:links
   {:self (link path-for item-ref)
    :lens/item-def (item-def/link path-for (:item-ref/item item-ref))}})

(defn render-embedded-list [path-for item-refs]
  (r/map #(render-embedded path-for %) item-refs))

(defnk render-list [item-group-def [:request path-for [:params page-num]]]
  (let [item-refs (->> (:item-group-def/item-refs item-group-def)
                       (sort-by (comp :item-def/name :item-ref/item)))
        next-page? (not (lr/empty? (hu/paginate (inc page-num) item-refs)))
        path #(path-for :item-refs-handler :eid (hu/entity-id item-group-def)
                        :page-num %)]
    {:links
     (-> {:up (item-group-def/link path-for item-group-def)
          :self {:href (path page-num)}}
         (hu/assoc-prev page-num path)
         (hu/assoc-next next-page? page-num path))

     :forms
     {:lens/create-item-ref
      (item-group-def/create-item-ref-form path-for item-group-def)}

     :embedded
     {:lens/item-refs
      (->> (hu/paginate page-num item-refs)
           (render-embedded-list path-for)
           (into []))}}))

(def list-handler
  "Resource of all item-refs of a item-group-def."
  (resource
    (item-group-def/child-list-resource-defaults)

    :handle-ok render-list))

(def find-item-ref
  (fnk [db [:request [:params eid item-id]]]
    (when-let [item-group-def (api/find-entity db :item-group-def (hu/to-eid eid))]
      (when-let [item-ref (some #(when (= item-id (-> % :item-ref/item
                                                      :item-def/id)) %)
                                (:item-group-def/item-refs item-group-def))]
        {:item-ref item-ref}))))

(def find-handler
  (resource
    (item-group-def/redirect-resource-defaults)

    :existed? find-item-ref

    :location
    (fnk [item-ref [:request path-for]] (path path-for item-ref))))

(defnk render [item-ref [:request path-for]]
  {:links
   {:up (item-group-def/link path-for (:item-group-def/_item-refs item-ref))
    :self (link path-for item-ref)
    :lens/item-def (item-def/link path-for (:item-ref/item item-ref))}

   :ops #{:delete}})

(def handler
  "Handler for GET and DELETE on an item-ref."
  (resource
    (hu/entity-resource-defaults)

    :allowed-methods [:get :delete]

    :exists? (hu/exists? :item-ref :item)

    :etag (hu/etag 1)

    :delete!
    (fnk [conn item-ref] (api/retract-entity conn (:db/id item-ref)))

    :handle-ok render))

(def ^:private CreateParamSchema
  {:item-id s/Str
   s/Any s/Any})

(defnk build-up-link [item-group-def [:request path-for]]
  {:links {:up (item-group-def/link path-for item-group-def)}})

(def create-handler
  (resource
    (item-group-def/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn item-group-def [:request [:params item-id]]]
      (let [study (:study/_item-group-defs item-group-def)]
        (if-let [item-def (api/find-study-child study :item-def item-id)]
          (if-let [item-ref (api/create-item-ref conn item-group-def item-def)]
            {:item-ref item-ref}
            (throw (ex-info "Duplicate!" {:type :duplicate})))
          (throw (ex-info "Item def not found." {:type :item-def-not-found})))))

    :location
    (fnk [item-ref [:request path-for]] (path path-for item-ref))

    :handle-exception
    (fnk [exception [:request path-for [:params item-id]] :as ctx]
      (condp = (util/error-type exception)
        :duplicate
        (hu/error path-for 409
                  (format "The item ref to item %s exists already." item-id)
                  (build-up-link ctx))
        :item-def-not-found
        (hu/error path-for 404 "Item def not found."
                  (build-up-link ctx))
        (throw exception)))))
