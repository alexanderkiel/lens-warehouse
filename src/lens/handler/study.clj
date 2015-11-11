(ns lens.handler.study
  (:use plumbing.core)
  (:require [clojure.core.reducers :as r]
            [clojure.string :as str]
            [liberator.core :refer [resource]]
            [lens.api :as api]
            [lens.pull :as pull]
            [lens.util :as util]
            [lens.handler.util :as hu]
            [lens.reducers :as lr]
            [schema.core :as s]))

(defn all-studies-path
  ([path-for] (all-studies-path path-for 1))
  ([path-for page-num] (path-for :all-studies-handler :page-num page-num)))

(defn path [path-for study]
  (path-for :study-handler :eid (hu/entity-id study)))

(defn link [path-for study]
  {:href (path path-for study) :label (:study/name study)})

(def find-handler
  (resource
    (hu/redirect-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (:id params))

    :existed?
    (fnk [db [:request [:params id]]]
      (when-let [study (api/find-study db id)]
        {:study study}))

    :location
    (fnk [study [:request path-for]]
      (path path-for study))))

(defn child-list-path
  ([child-type path-for study] (child-list-path child-type path-for study 1))
  ([child-type path-for study page-num]
   (let [list-handler (keyword (str "study-" (name child-type) "s-handler"))]
     (path-for list-handler :eid (hu/entity-id study) :page-num page-num))))

(defn upper-first [s]
  (apply str (str/upper-case (str (first s))) (rest s)))

(defn show [child-type]
  (str/join " " (map upper-first (str/split (name child-type) #"-"))))

(defn child-list-link
  ([child-type path-for study] (child-list-link child-type path-for study 1))
  ([child-type path-for study page-num]
   {:href (child-list-path child-type path-for study page-num)
    :label (str (show child-type) "s - Page " page-num)}))

(defn child-action-path [child-type action path-for study]
  (let [handler (keyword (str (name action) "-" (name child-type) "-handler"))]
    (path-for handler :eid (hu/entity-id study))))

(defn- render-child-find-query [child-type path-for study]
  {:href (child-action-path child-type :find path-for study)
   :params {:id {:type s/Str}}})

(defn render-create-study-form [path-for]
  {:href (path-for :create-study-handler)
   :params
   {:id {:type s/Str :desc "The id has to be unique within the whole system."}
    :name {:type s/Str :desc "A short name for the study."}
    :desc
    {:type s/Str
     :label "Description"
     :desc "A free-text description of the study."}}})

(defn render-create-study-event-def-form [path-for study]
  {:href (child-action-path :study-event-def :create path-for study)
   :params
   {:id {:type s/Str}
    :name {:type s/Str}
    :desc {:type s/Str :optional true}}})

(defn render-create-form-def-form [path-for study]
  {:href (child-action-path :form-def :create path-for study)
   :params
   {:id {:type s/Str}
    :name {:type s/Str}
    :desc {:type s/Str :optional true}}})

(defn render-create-item-group-create-form [path-for study]
  {:href (child-action-path :item-group-def :create path-for study)
   :params
   {:id {:type s/Str}
    :name {:type s/Str}
    :desc {:type s/Str :optional true}}})

(def item-def-data-type-schema (s/enum :text :integer :float :date :time
                                       :datetime :string :boolean :double))

(defn render-create-item-def-form [path-for study]
  {:href (child-action-path :item-def :create path-for study)
   :params
   {:id {:type s/Str}
    :name {:type s/Str}
    :data-type {:type item-def-data-type-schema}
    :desc {:type s/Str :optional true}
    :question {:type s/Str :optional true}
    :length {:type s/Int :optional true}}})

(defn render-create-subject-form [path-for study]
  {:href (child-action-path :subject :create path-for study)
   :params
   {:id {:type s/Str}}})

(defnk render [study [:request path-for]]
  {:data
   {:id (:study/id study)
    :name (:study/name study)
    :desc (:study/desc study)}

   :links
   {:up {:href (path-for :service-document-handler)}
    :self (link path-for study)
    :profile {:href (path-for :study-profile-handler)}
    :lens/study-event-defs
    (child-list-link :study-event-def path-for study)
    :lens/form-defs
    (child-list-link :form-def path-for study)
    :lens/item-group-defs
    (child-list-link :item-group-def path-for study)
    :lens/item-defs
    (child-list-link :item-def path-for study)}

   :queries
   {:lens/find-study-event-def
    (render-child-find-query :study-event-def path-for study)

    :lens/find-form-def
    (render-child-find-query :form-def path-for study)

    :lens/find-item-group-def
    (render-child-find-query :item-group-def path-for study)

    :lens/find-item-def
    (render-child-find-query :item-def path-for study)}

   :forms
   {:lens/create-study-event-def
    (render-create-study-event-def-form path-for study)

    :lens/create-form-def
    (render-create-form-def-form path-for study)

    :lens/create-item-group-def
    (render-create-item-group-create-form path-for study)

    :lens/create-item-def
    (render-create-item-def-form path-for study)

    :lens/create-subject
    (render-create-subject-form path-for study)}

   :ops #{:update :delete}})

(def select-study-props (hu/select-props :study :name :desc))

(def schema {:name s/Str :desc s/Str})

(def handler
  "Handler for GET, PUT and DELETE on a study.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the study taken from a database outside of the
  transaction. The update transaction is than tried with name and desc from that
  possibly old study as reference. The transaction only succeeds if the name and
  desc are still the same on the in-transaction study."
  (resource
    (hu/entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      (let [study (api/find-entity db :study (hu/to-eid db eid))]
        ((hu/entity-processable (assoc schema :id (s/eq (:study/id study)))) ctx)))

    :exists? (hu/exists? :study)

    :etag
    (hu/etag #(-> % :study :study/name)
             #(-> % :study :study/desc)
             2)

    :put!
    (fnk [conn study new-entity]
      (let [new-entity (util/prefix-namespace :study new-entity)]
        {:update-error (api/update-study conn (:study/id study)
                                         (select-study-props study)
                                         (select-study-props new-entity))}))

    :delete!
    (fnk [conn study] (api/retract-entity conn (:db/id study)))

    :handle-ok render))

(defn render-embedded-study [path-for study]
  {:data
   {:id (:study/id study)
    :name (:study/name study)
    :desc (:study/desc study)}
   :links
   {:self (link path-for study)}})

(defn render-embedded-studies [path-for studies]
  (r/map #(render-embedded-study path-for %) studies))

(def ListParamSchema
  {:page-num util/PosInt
   (s/optional-key :pull-pattern) pull/Pattern
   (s/optional-key :filter) s/Str
   s/Any s/Any})

(defn db-pull-pattern [pull-pattern]
  (let [default [:db/id :study/id :study/name]]
    (if (some #{:desc} (first (map :data pull-pattern)))
      (conj default :study/desc)
      default)))

(def all-studies-handler
  (resource
    (hu/resource-defaults)

    :processable? (hu/coerce-params ListParamSchema)

    ;;TODO: ETag

    :handle-ok
    (fnk [db [:request path-for [:params page-num {pull-pattern nil} {filter nil}]]]
      (let [studies (if (str/blank? filter)
                      (api/all-studies db (db-pull-pattern pull-pattern))
                      (api/list-matching-studies db filter))
            next-page? (not (lr/empty? (hu/paginate (inc page-num) studies)))
            path #(-> (all-studies-path path-for %)
                      (hu/assoc-filter filter))]
        {:links
         (-> {:up {:href (path-for :service-document-handler)}
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))
         :forms
         {:lens/create-study
          (render-create-study-form path-for)}
         :embedded
         {:lens/studies
          (->> (hu/paginate page-num studies)
               (render-embedded-studies path-for)
               (into []))}}))))

(def ^:private CreateParamSchema
  {:id util/NonBlankStr
   :name util/NonBlankStr
   :desc s/Str
   s/Any s/Any})

(def create-handler
  (resource
    (hu/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn [:request [:params id name desc]]]
      (if-let [study (api/create-study conn id name desc)]
        {:study study}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location (fnk [study [:request path-for]] (path path-for study))

    :handle-exception (hu/duplicate-exception "Study exists already.")))

;; ---- For Childs ------------------------------------------------------------

(def ^:private ChildListParamSchema
  {:eid util/Base62EntityId
   :page-num util/PosInt
   s/Any s/Any})

(defn child-list-resource-defaults []
  (assoc
    (hu/resource-defaults)

    :processable? (hu/coerce-params ChildListParamSchema)

    :exists? (hu/exists? :study)))

(defnk build-up-link [[:request path-for [:params eid]]]
  {:links {:up {:href (path-for :study-handler :eid eid)}}})

(def ^:private RedirectParamSchema
  {:id util/NonBlankStr
   s/Any s/Any})

(defn redirect-resource-defaults []
  (assoc
    (hu/redirect-resource-defaults)

    :processable? (hu/validate-params RedirectParamSchema)

    :handle-unprocessable-entity
    (hu/error-handler "Unprocessable Entity" build-up-link)))

(defn create-resource-defaults []
  (assoc
    (hu/create-resource-defaults)

    :exists? (hu/exists? :study)))

(defn duplicate-exception [msg]
  (hu/duplicate-exception msg build-up-link))

(defn find-child [type]
  (fnk [db [:request [:params eid id]]]
    (when-let [study (api/find-entity db :study (hu/to-eid db eid))]
      (when-let [entity (api/find-study-child study type id)]
        {:entity entity}))))

(defn find-child-handler [type]
  (resource
    (redirect-resource-defaults)

    :existed? (find-child type)

    :location
    (fnk [entity [:request path-for]]
      (path-for (keyword (str (name type) "-handler")) :eid (hu/entity-id entity)))))
