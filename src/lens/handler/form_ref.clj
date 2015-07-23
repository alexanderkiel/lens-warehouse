(ns lens.handler.form-ref
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [schema.core :as s]
            [lens.handler.util :as hu]
            [lens.handler.study-event-def :as study-event-def]
            [lens.api :as api]
            [lens.util :as util]
            [lens.handler.form-def :as form-def]
            [lens.reducers :as lr]
            [clojure.core.reducers :as r]))

(defn- path
  ([path-for form-ref]
   (path-for :form-ref-handler :eid (hu/entity-id form-ref))))

(defn link [path-for form-ref]
  {:href (path path-for form-ref)
   :label (str "Form Ref " (:form-def/name (:form-ref/form form-ref)))})

(defn render-embedded [path-for form-ref]
  {:links
   {:self (link path-for form-ref)
    :lens/form-def (form-def/link path-for (:form-ref/form form-ref))}})

(defn render-embedded-list [path-for form-refs]
  (r/map #(render-embedded path-for %) form-refs))

(defnk render-list [study-event-def [:request path-for [:params page-num]]]
  (let [form-refs (->> (:study-event-def/form-refs study-event-def)
                       (sort-by (comp :form-def/name :form-ref/form)))
        next-page? (not (lr/empty? (hu/paginate (inc page-num) form-refs)))
        path #(path-for :form-refs-handler :eid (hu/entity-id study-event-def)
                        :page-num %)]
    {:links
     (-> {:up (study-event-def/link path-for study-event-def)
          :self {:href (path page-num)}}
         (hu/assoc-prev page-num path)
         (hu/assoc-next next-page? page-num path))

     :forms
     {:lens/create-form-ref
      (study-event-def/create-form-ref-form path-for study-event-def)}

     :embedded
     {:lens/form-refs
      (->> (hu/paginate page-num form-refs)
           (render-embedded-list path-for)
           (into []))}}))

(def list-handler
  "Resource of all form-refs of a study-event-def."
  (resource
    (study-event-def/child-list-resource-defaults)

    :handle-ok render-list))

(def find-form-ref
  (fnk [db [:request [:params eid form-id]]]
    (when-let [study-event-def (api/find-entity db :study-event-def (hu/to-eid db eid))]
      (when-let [form-ref (some #(when (= form-id (-> % :form-ref/form
                                                      :form-def/id)) %)
                                (:study-event-def/form-refs study-event-def))]
        {:form-ref form-ref}))))

(def find-handler
  (resource
    (study-event-def/redirect-resource-defaults)

    :existed? find-form-ref

    :location
    (fnk [form-ref [:request path-for]] (path path-for form-ref))))

(defnk render [form-ref [:request path-for]]
  {:links
   {:up (study-event-def/link path-for (:study-event-def/_form-refs form-ref))
    :self (link path-for form-ref)
    :lens/form-def (form-def/link path-for (:form-ref/form form-ref))}

   :ops #{:delete}})

(def handler
  "Handler for GET and DELETE on an form-ref."
  (resource
    (hu/standard-entity-resource-defaults)

    :allowed-methods [:get :delete]

    :exists? (hu/exists? :form-ref :form)

    :etag (fnk [representation] (hu/md5 (:media-type representation)))

    :delete!
    (fnk [conn form-ref] (api/retract-entity conn (:db/id form-ref)))

    :handle-ok render))

(def ^:private CreateParamSchema
  {:form-id s/Str
   s/Any s/Any})

(defnk build-up-link [study-event-def [:request path-for]]
  {:links {:up (study-event-def/link path-for study-event-def)}})

(def create-handler
  (resource
    (study-event-def/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn study-event-def [:request [:params form-id]]]
      (let [study (:study/_study-event-defs study-event-def)]
        (if-let [form-def (api/find-study-child study :form-def form-id)]
          (if-let [form-ref (api/create-form-ref conn study-event-def form-def)]
            {:form-ref form-ref}
            (throw (ex-info "Duplicate!" {:type :duplicate})))
          (throw (ex-info "Form def not found." {:type :form-def-not-found})))))

    :location
    (fnk [form-ref [:request path-for]] (path path-for form-ref))

    :handle-exception
    (fnk [exception [:request path-for] :as ctx]
      (condp = (util/error-type exception)
        :duplicate
        (hu/error path-for 409 "The form ref exists already."
                  (build-up-link ctx))
        :form-def-not-found
        (hu/error path-for 409 "Form def not found."
                  (build-up-link ctx))
        (throw exception)))))
