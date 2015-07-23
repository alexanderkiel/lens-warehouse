(ns lens.handler.form-def
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

(defn path [path-for form-def]
  (path-for :form-def-handler :eid (hu/entity-id form-def)))

(defn link [path-for form-def]
  {:href (path path-for form-def)
   :label (str "Form Def " (:form-def/name form-def))})

(defn render-embedded [path-for timeout form-def]
  (-> {:id (:form-def/id form-def)
       ;;TODO: alias
       :name (:name form-def)
       :links
       {:self
        (link path-for form-def)}}
      #_(assoc-count
        (util/try-until timeout (api/num-form-subjects form-def))
        (path-for :form-count-handler :id (:form-def/id form-def)))))

(defn render-embedded-list [path-for timeout form-defs]
  (r/map #(render-embedded path-for timeout %) form-defs))

(defnk render-list [study [:request path-for [:params page-num {filter nil}]]]
  (let [form-defs (if (str/blank? filter)
                    (->> (:study/form-defs study)
                         (sort-by :form-def/id))
                    (api/list-matching-form-defs study filter))
        next-page? (not (lr/empty? (hu/paginate (inc page-num) form-defs)))
        path #(-> (study/child-list-path :form-def path-for study %)
                  (hu/assoc-filter filter))]
    {:links
     (-> {:up (study/link path-for study)
          :self {:href (path page-num)}}
         (hu/assoc-prev page-num path)
         (hu/assoc-next next-page? page-num path))

     :queries
     {:lens/filter
      (hu/render-filter-query (study/child-list-path :form-def path-for study))}

     :forms
     {:lens/create-form-def
      (study/render-create-form-def-form path-for study)}

     :embedded
     {:lens/form-defs
      (->> (hu/paginate page-num form-defs)
           (render-embedded-list path-for (timeout 100))
           (into []))}}))

(def list-handler
  "Resource of all form-defs of a study."
  (resource
    (study/child-list-resource-defaults)

    :handle-ok render-list))

(defn- find-item-group-ref-path [path-for form-def]
  (path-for :find-item-group-ref-handler :eid (hu/entity-id form-def)))

(defn- item-group-refs-path [path-for form-def]
  (path-for :item-group-refs-handler :eid (hu/entity-id form-def) :page-num 1))

(defn- create-item-group-ref-path [path-for form-def]
  (path-for :create-item-group-ref-handler :eid (hu/entity-id form-def)))

(defn create-item-group-ref-form [path-for form-def]
  {:href (create-item-group-ref-path path-for form-def)
   :params {:form-id {:type s/Str}}})

(def select-props (hu/select-props :form-def :name :desc))

(defnk render [form-def [:request path-for]]
  {:data
   (-> {:id (:form-def/id form-def)
        ;;TODO: alias
        :name (:form-def/name form-def)}
       (assoc-when :desc (:form-def/desc form-def)))

   :links
   {:up (study/link path-for (:study/_form-defs form-def))
    :self (link path-for form-def)
    :profile {:href (path-for :form-def-profile-handler)}
    :lens/item-group-refs {:href (item-group-refs-path path-for form-def)}}

   :queries
   {:lens/find-item-group-ref
    {:href (find-item-group-ref-path path-for form-def)
     :params {:item-group-id {:type s/Str}}}}

   :forms
   {:lens/create-item-group-ref
    (create-item-group-ref-form path-for form-def)}

   :ops #{:update :delete}})

(def schema {:name s/Str (s/optional-key :desc) s/Str})

(def handler
  "Handler for GET, PUT and DELETE on a form-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the form-def taken from a database outside of the
  transaction. The update transaction is than tried with name and desc
  from that possibly old form-def as reference. The transaction only succeeds if
  the name and desc are still the same on the in-transaction form-def."
  (resource
    (hu/standard-entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      (let [form-def (api/find-entity db :form-def (hu/to-eid db eid))]
        ((hu/entity-processable (assoc schema :id (s/eq (:form-def/id form-def)))) ctx)))

    :exists? (hu/exists? :form-def)

    :etag
    (fnk [representation :as ctx]
      (when-let [form-def (:form-def ctx)]
        (hu/md5 (:media-type representation)
                (:form-def/name form-def)
                (:form-def/desc form-def))))

    :put!
    (fnk [conn form-def new-entity]
      (let [new-entity (util/prefix-namespace :form-def new-entity)]
        {:update-error (api/update-form-def conn form-def (select-props form-def)
                                            (select-props new-entity))}))

    :delete!
    (fnk [conn form-def] (api/retract-entity conn (:db/id form-def)))

    :handle-ok render))

(defn form-def-count-handler [path-for]
  (resource
    (hu/resource-defaults)

    :exists? (hu/exists? :form-def)

    :handle-ok
    (fnk [entity]
      {:value (api/num-form-subjects entity)
       :links
       {:up {:href (path path-for entity)}
        :self {:href (path-for :form-count-handler :id (:form/id entity))}}})

    :handle-not-found
    (hu/error-body path-for "Form not found.")))

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
                      (util/prefix-namespace :form-def))]
        (if-let [entity (api/create-form-def conn study id name opts)]
          {:entity entity}
          (throw (ex-info "Duplicate!" {:type :duplicate})))))

    :location
    (fnk [entity [:request path-for]] (path path-for entity))

    :handle-exception
    (study/duplicate-exception "The form def exists already.")))

;; ---- For Childs ------------------------------------------------------------

(defnk build-up-link [[:request path-for [:params eid]]]
  {:links {:up {:href (path-for :form-def-handler :eid eid)}}})

(def ^:private ChildListParamSchema
  {:eid util/Base62EntityId
   :page-num util/PosInt
   s/Any s/Any})

(defn child-list-resource-defaults []
  (assoc
    (hu/resource-defaults)

    :processable? (hu/coerce-params ChildListParamSchema)

    :exists? (hu/exists? :form-def)))

(defn redirect-resource-defaults []
  (assoc
    (hu/redirect-resource-defaults)

    :handle-unprocessable-entity
    (hu/error-handler "Unprocessable Entity" build-up-link)))

(defn create-resource-defaults []
  (assoc
    (hu/create-resource-defaults)

    :exists? (hu/exists? :form-def)))
