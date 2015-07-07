(ns lens.handler
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource to-location]]
            [lens.handler.util :refer :all]
            [lens.api :as api]
            [lens.util :as util]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.edn :as edn]
            [datomic.api :as d]
            [cemerick.url :refer [url url-encode url-decode]]
            [lens.handler.study :refer :all]
            [lens.handler.study-event-def :as study-event-def]
            [lens.handler.form-def :as form-def]
            [lens.handler.item-group-def :as item-group-def]
            [lens.handler.item-def :as item-def]
            [schema.core :as s])
  (:import [java.util UUID]))

;; ---- Service Document ------------------------------------------------------

(defn render-service-document [version]
  (fnk [[:request path-for]]
    {:data
     {:name "Lens Warehouse"
      :version version}
     :links
     {:self {:href (path-for :service-document-handler)}
      :lens/all-studies {:href (all-studies-path path-for)}
      :lens/all-snapshots {:href (path-for :all-snapshots-handler)}
      :lens/most-recent-snapshot {:href (path-for :most-recent-snapshot-handler)}}
     :queries
     {:lens/find-study
      {:href (path-for :find-study-handler)
       :params
       {:id
        {:type s/Str}}}}
     :forms
     {:lens/create-study
      (render-create-study-form path-for)}}))

(defn service-document-handler [version]
  (resource
    (resource-defaults :cache-control "max-age=60")

    :etag
    (fnk [representation [:request path-for]]
      (md5 (str (:media-type representation)
                (path-for :service-document-handler)
                (all-studies-path path-for)
                (path-for :all-snapshots-handler)
                (path-for :most-recent-snapshot-handler)
                (path-for :find-study-handler)
                (path-for :create-study-handler))))

    :handle-ok (render-service-document version)))

;; ---- Form Refs --------------------------------------------------------------

(defn- form-ref-path
  ([path-for form-ref]
   (form-ref-path path-for (:study/id (:study/_form-refs form-ref))
                  (:form-ref/id form-ref)))
  ([path-for study-id form-ref-id]
   (path-for :form-ref-handler :study-id study-id :form-ref-id form-ref-id)))


;; ---- Form Ref --------------------------------------------------------------

(def create-form-ref-handler
  (resource
    (standard-create-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:study-event-def-id params) (:form-id params)
           (:order-number params)))

    :exists? (fn [ctx] (some-> (exists-study? ctx) (study-event-def/exists?)))

    #_:post!
    #_(fnk [conn study-event-def [:request params]]
      (if-let [form-ref (api/create-form-ref conn study-event-def 
                                             (:form-id params)
                                             (:order-number params))]
        {:form-ref form-ref}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [form-ref [:request path-for]] (form-ref-path path-for form-ref))

    :handle-exception (duplicate-exception "Form-def exists already.")))

(defn item-code-list-item-count-handler [path-for]
  (resource
    (resource-defaults)

    :exists?
    (fnk [db [:request [:params id code]]]
      (when-let [item (api/find-study-child db :item-def id)]
        (when-let [code-list-item (api/code-list-item item code)]
          {:item item
           :code-list-item code-list-item})))

    :handle-ok
    (fnk [item code-list-item]
      {:value (api/num-code-list-item-subjects code-list-item)
       :links
       {:up {:href (path-for :item-def-handler :id (:item/id item))}
        :self {:href (path-for :item-code-list-item-count-handler
                               :id (:item/id item)
                               :code (api/code code-list-item))}}})

    :handle-not-found
    (fnk [[:request [:params id code]]]
      (error-body path-for (str "Item " id " has no code list item with code "
                                code ".")))))

;; ---- Code-List -------------------------------------------------------------

(defn render-embedded-code-list-item [code-list-item]
  {:code (api/code code-list-item)
   :label (:code-list-item/label code-list-item)
   :count "?"
   :type :code-list-item})

(defn render-embedded-code-list-items [code-list-items]
  (mapv render-embedded-code-list-item code-list-items))

(defn code-list-handler [path-for]
  (resource
    (resource-defaults)

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [code-list (api/code-list db id)]
        {:code-list code-list}))

    :handle-ok
    (fnk [code-list]
      (-> {:id (:code-list/id code-list)
           :type :code-list
           :links
           {:up {:href (path-for :service-document-handler)}
            :self {:href (path-for :code-list-hanlder :id
                                   (:code-list/id code-list))}}
           :embedded
           {:lens/code-list-items
            (->> (api/code-list-items code-list)
                 ;; TODO: use :code-list-item/rank instead
                 (sort-by (:code-list/attr code-list))
                 (render-embedded-code-list-items))}}
          (assoc-when :name (:name code-list))))

    :handle-not-found
    (error-body path-for "Code List not found.")))

;; ---- Subject ---------------------------------------------------------------

(defn subject-path [path-for subject]
  (path-for :subject-handler :study-id (:study/id (:subject/study subject))
            :subject-id (:subject/id subject)))

(defnk exists-subject? [study [:request [:params subject-id]]]
  (when-let [subject (api/find-subject study subject-id)]
    {:subject subject}))

(defnk render-subject [subject [:request path-for]]
  {:data
   {:id (:subject/id subject)}
   :links
   {:up {:href (path-for :service-document-handler)}
    :self {:href (subject-path path-for subject)}}})

(def subject-handler
  (resource
    (resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:subject-id params)))

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-subject?)))

    :handle-ok render-subject

    :handle-not-found
    (fnk [[:request path-for]] (error-body path-for "Subject not found."))))

(def create-subject-handler
  (resource
    (standard-create-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params)))

    :exists? exists-study?

    :post!
    (fnk [conn study [:request [:params id]]]
      (if-let [subject (api/create-subject conn study id)]
        {:subject subject}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [subject [:request path-for]] (subject-path path-for subject))

    :handle-exception (duplicate-exception "Subject exists already.")))

(defn delete-subject-handler [path-for]
  (fnk [conn [:params id]]
    (if (api/retract-subject conn id)
      {:status 204}
      (ring-error path-for 404 "Subject not found."))))

;; ---- Snapshots -------------------------------------------------------------

(defn snapshot-path [path-for snapshot]
  (path-for :snapshot-handler :id (:tx-id snapshot)))

(defn render-embedded-snapshot [path-for snapshot]
  {:links
   {:self {:href (snapshot-path path-for snapshot)}}
   :id (str (:tx-id snapshot))
   :time (:db/txInstant snapshot)})

(defn render-embedded-snapshots [path-for snapshots]
  (mapv #(render-embedded-snapshot path-for %) snapshots))

(defn all-snapshots-handler [path-for]
  (resource
    (resource-defaults)

    :handle-ok
    (fnk [db]
      (let [snapshots (->> (api/all-snapshots db)
                           (into [])
                           (sort-by :db/txInstant)
                           (reverse))]
        {:links
         {:self {:href (path-for :all-snapshots-handler)}
          :up {:href (path-for :service-document-handler)}}
         :embedded
         {:lens/snapshots
          (render-embedded-snapshots path-for snapshots)}}))))

(defn snapshot-handler [path-for]
  (resource
    (resource-defaults)

    :processable?
    (fnk [[:request params]]
      (some->> (:id params) (re-matches util/uuid-regexp)))

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [snapshot (api/snapshot db (UUID/fromString id))]
        {:snapshot snapshot}))

    :handle-ok
    (fnk [snapshot]
      {:links
       {:self {:href (snapshot-path path-for snapshot)}}
       :id (str (:tx-id snapshot))
       :time (:db/txInstant snapshot)
       :queries
       {:lens/query
        {:href (path-for :query-handler :id (:tx-id snapshot))
         :title "Query"
         :params
         {:expr
          {:type s/Str
           :desc "Issues a query against the database.

    The query consists of two parts. Part one specifies the :items which have to
    be present and part two specifies the :study-events which are of interest.

    The first part consists of a seq of disjunctions forming one conjunction.
    Each disjunction is a seq of atoms. Each atom has a type and an identifier.

    The second part consists of a seq of of study-event identifiers. An empty
    seq returns all visits regardless of study-event.

    Valid types are:

    * :form
    * :item-group
    * :item
    * :code-list-item

    The identifiers are values of the corresponding :<type>/id attribute of the
    schema except for :code-list-item where the identifier is a map of :item-id
    and :code.

    Example atoms:

    [:form \"T00001\"]
    [:item-group \"cdd90833-d9c3-4ba1-b21e-7d03483cae63\"]
    [:item \"T00001_F0001\"]
    [:code-list-item {:item-id \"T00001_F0001\" :code 0}]

    Example query:

    {:items [[[:form \"T00001\"]]]
     :study-events [\"A1_HAUPT01\"]}

    The query returns the set of visits which has data points satisfying the
    query."}}}}})))

(defn most-recent-snapshot-handler [path-for]
  (resource
    (resource-defaults)

    :exists? false

    :existed?
    (fnk [db]
      (some->> (api/all-snapshots db)
               (into [])
               (sort-by :db/txInstant)
               (last)
               (hash-map :snapshot)))

    :moved-temporarily? true

    :handle-moved-temporarily
    (fnk [snapshot]
      (-> (snapshot-path path-for snapshot)
          (to-location)))

    :handle-not-found
    (error-body path-for "No snapshot found.")))

;; ---- Query -----------------------------------------------------------------

(defn visit-count-by-study-event [visits]
  (->> (group-by :visit/study-event visits)
       (map-keys :study-event/id)
       (map-vals count)))

(defn age-at-visit [visit]
  (when-let [birth-date (-> visit :visit/subject :subject/birth-date)]
    (when-let [edat (:visit/edat visit)]
      (if (t/after? (tc/from-date edat) (tc/from-date birth-date))
        (t/in-years (t/interval (tc/from-date birth-date) (tc/from-date edat)))
        (- (t/in-years (t/interval (tc/from-date edat) (tc/from-date birth-date))))))))

(defn sex [visit]
  (some-> visit :visit/subject :subject/sex name keyword))

(defn age-decade [age]
  {:pre [age]}
  (* (quot age 10) 10))

(defn visit-count-by-age-decade-and-sex [visits]
  (->> (group-by #(some-> (age-at-visit %) age-decade) visits)
       (reduce-kv
         (fn [r age-decade visits]
           (if age-decade
             (assoc r age-decade (->> (r/map sex visits)
                                      (r/remove nil?)
                                      (frequencies)))
             r))
         {})))

(defn subject-count [visits]
  (->> (r/map :visit/subject visits)
       (into #{})
       (count)))

(defn query-handler [path-for]
  (resource
    (resource-defaults :cache-control "max-age=3600")

    :processable?
    (fnk [[:request params]]
      (when (some->> (:id params) (re-matches util/uuid-regexp))
        (when-let [expr (:expr params)]
          (try
            {:expr (edn/read-string expr)}
            (catch Exception _)))))

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [snapshot (api/snapshot db (UUID/fromString id))]
        {:snapshot snapshot
         :db (d/as-of db (:db/id snapshot))}))

    :etag
    (fnk [snapshot [:representation media-type]]
      (md5 (str media-type (snapshot-path path-for snapshot))))

    :handle-ok
    (fnk [snapshot db expr]
      (let [visits (api/query db expr)]
        {:links {:up {:href (snapshot-path path-for snapshot)}}
         :visit-count (count visits)
         :visit-count-by-study-event (visit-count-by-study-event visits)
         :visit-count-by-age-decade-and-sex
         (visit-count-by-age-decade-and-sex visits)
         :subject-count (subject-count visits)}))))

;; ---- Handlers --------------------------------------------------------------

(defnk handlers [path-for version]
  {:service-document-handler (service-document-handler version)

   :all-studies-handler all-studies-handler
   :find-study-handler find-study-handler
   :study-handler study-handler
   :create-study-handler create-study-handler
   :study-profile-handler study-profile-handler

   :study-study-event-defs-handler study-event-def/list-handler
   :find-study-event-def-handler study-event-def/find-handler
   :study-event-def-handler study-event-def/handler
   :create-study-event-def-handler study-event-def/create-handler

   ;:find-form-ref-handler find-form-ref-handler
   :create-form-ref-handler create-form-ref-handler

   :study-form-defs-handler form-def/list-handler
   :find-form-def-handler form-def/find-handler
   :form-def-handler form-def/handler
   :form-count-handler (form-def/form-def-count-handler path-for)
   :create-form-def-handler form-def/create-handler

   :study-item-group-defs-handler item-group-def/list-handler
   :find-item-group-def-handler item-group-def/find-handler
   :item-group-def-handler item-group-def/handler
   :item-group-count-handler (item-group-def/item-group-def-count-handler path-for)
   :create-item-group-def-handler item-group-def/create-handler

   :study-item-defs-handler item-def/list-handler
   :find-item-def-handler item-def/find-handler
   :item-def-handler item-def/handler
   :item-def-count-handler (item-def/item-def-count-handler path-for)
   :create-item-def-handler item-def/create-handler

   :item-code-list-item-count-handler
   (item-code-list-item-count-handler path-for)
   :code-list-handler (code-list-handler path-for)

   :subject-handler subject-handler
   :create-subject-handler create-subject-handler
   :delete-subject-handler (delete-subject-handler path-for)

   :query-handler (query-handler path-for)
   :snapshot-handler (snapshot-handler path-for)
   :all-snapshots-handler (all-snapshots-handler path-for)
   :most-recent-snapshot-handler (most-recent-snapshot-handler path-for)})
