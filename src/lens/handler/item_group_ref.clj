(ns lens.handler.item-group-ref
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [schema.core :as s]
            [lens.handler.util :as hu]
            [lens.handler.form-def :as form-def]
            [lens.api :as api]
            [lens.util :as util]
            [lens.reducers :as lr]
            [clojure.core.reducers :as r]
            [lens.handler.item-group-def :as item-group-def]))

(defn- path
  ([path-for item-group-ref]
   (path-for :item-group-ref-handler :eid (hu/entity-id item-group-ref))))

(defn link [path-for item-group-ref]
  {:href (path path-for item-group-ref)
   :label (-> item-group-ref :item-group-ref/item-group :item-group-def/name)})

(defn render-embedded [path-for item-group-ref]
  {:links
   {:self (link path-for item-group-ref)
    :lens/item-group-def
    (item-group-def/link path-for (:item-group-ref/item-group item-group-ref))}})

(defn render-embedded-list [path-for item-group-refs]
  (r/map #(render-embedded path-for %) item-group-refs))

(defnk render-list [form-def [:request path-for [:params page-num]]]
  (let [item-group-refs (->> (:form-def/item-group-refs form-def)
                             (sort-by (comp :form-def/name :item-group-ref/item-group)))
        next-page? (not (lr/empty? (hu/paginate (inc page-num) item-group-refs)))
        path #(path-for :item-group-refs-handler :eid (hu/entity-id form-def)
                        :page-num %)]
    {:links
     (-> {:up (form-def/link path-for form-def)
          :self {:href (path page-num)}}
         (hu/assoc-prev page-num path)
         (hu/assoc-next next-page? page-num path))

     :forms
     {:lens/create-item-group-ref
      (form-def/create-item-group-ref-form path-for form-def)}

     :embedded
     {:lens/item-group-refs
      (->> (hu/paginate page-num item-group-refs)
           (render-embedded-list path-for)
           (into []))}}))

(def list-handler
  "Resource of all item-group-refs of a form-def."
  (resource
    (form-def/child-list-resource-defaults)

    :handle-ok render-list))

(def find-item-group-ref
  (fnk [db [:request [:params eid item-group-id]]]
    (when-let [form-def (api/find-entity db :form-def (hu/to-eid db eid))]
      (when-let [item-group-ref (some #(when (= item-group-id (-> % :item-group-ref/item-group
                                                                  :item-group-def/id)) %)
                                      (:form-def/item-group-refs form-def))]
        {:item-group-ref item-group-ref}))))

(def find-handler
  (resource
    (form-def/redirect-resource-defaults)

    :existed? find-item-group-ref

    :location
    (fnk [item-group-ref [:request path-for]] (path path-for item-group-ref))))

(defnk render [item-group-ref [:request path-for]]
  {:links
   {:up (form-def/link path-for (:form-def/_item-group-refs item-group-ref))
    :self (link path-for item-group-ref)
    :lens/item-group-def (item-group-def/link path-for (:item-group-ref/item-group item-group-ref))}

   :ops #{:delete}})

(def handler
  "Handler for GET and DELETE on an item-group-ref."
  (resource
    (hu/entity-resource-defaults)

    :allowed-methods [:get :delete]

    :exists? (hu/exists? :item-group-ref :item-group)

    :etag (hu/etag 1)

    :delete!
    (fnk [conn item-group-ref] (api/retract-entity conn (:db/id item-group-ref)))

    :handle-ok render))

(def ^:private CreateParamSchema
  {:item-group-id s/Str
   s/Any s/Any})

(defnk build-up-link [form-def [:request path-for]]
  {:links {:up (form-def/link path-for form-def)}})

(def create-handler
  (resource
    (form-def/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn form-def [:request [:params item-group-id]]]
      (let [study (:study/_form-defs form-def)]
        (if-let [item-group-def (api/find-study-child study :item-group-def item-group-id)]
          (if-let [item-group-ref (api/create-item-group-ref conn form-def item-group-def)]
            {:item-group-ref item-group-ref}
            (throw (ex-info "Duplicate!" {:type :duplicate})))
          (throw (ex-info "Form def not found." {:type :form-def-not-found})))))

    :location
    (fnk [item-group-ref [:request path-for]] (path path-for item-group-ref))

    :handle-exception
    (fnk [exception [:request path-for [:params item-group-id]] :as ctx]
      (condp = (util/error-type exception)
        :duplicate
        (hu/error path-for 409
                  (format "The item group ref %s exists already." item-group-id)
                  (build-up-link ctx))
        :form-def-not-found
        (hu/error path-for 404 "Item group def not found."
                  (build-up-link ctx))
        (throw exception)))))
