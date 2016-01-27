(ns lens.handler
  (:use plumbing.core)
  (:require [async-error.core :refer [<??]]
            [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource to-location]]
            [lens.handler.util :as hu]
            [lens.api :as api]
            [lens.search.api :as sapi]
            [lens.util :as util]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.edn :as edn]
            [datomic.api :as d]
            [cemerick.url :refer [url url-encode url-decode]]
            [lens.handler.subject :as subject]
            [lens.handler.study :as study]
            [lens.handler.study-event-def :as study-event-def]
            [lens.handler.form-ref :as form-ref]
            [lens.handler.form-def :as form-def]
            [lens.handler.item-group-ref :as item-group-ref]
            [lens.handler.item-group-def :as item-group-def]
            [lens.handler.item-ref :as item-ref]
            [lens.handler.item-def :as item-def]
            [lens.handler.attachment-type :as attachment-type]
            [schema.core :as s]
            [lens.handler.util :as hu]
            [lens.pull :as pull])
  (:import [java.util UUID]))

;; ---- Service Document ------------------------------------------------------

(defn render-service-document [version]
  (fnk [[:request path-for]]
    {:data
     {:name "Lens Warehouse"
      :version version}
     :links
     {:self {:href (path-for :service-document-handler)}
      :lens/all-studies {:href (study/all-studies-path path-for)}
      :lens/all-snapshots {:href (path-for :all-snapshots-handler)}
      :lens/all-attachment-types
      {:href (attachment-type/all-attachment-types-path path-for)}
      :lens/most-recent-snapshot
      {:href (path-for :most-recent-snapshot-handler)}
      :datomic/basis-t {:href (path-for :basis-t-handler)}
      :life/health {:href (path-for :health-handler)}}
     :queries
     {:lens/find-study
      {:href (path-for :find-study-handler)
       :params
       {:id
        {:type s/Str}}}
      :lens/all-studies
      {:href (study/all-studies-path path-for)
       :params
       {:pull-pattern
        {:type pull/Pattern}}}}
     :forms
     {:lens/create-study
      (study/render-create-study-form path-for)}}))

(defn service-document-handler [version]
  (resource
    (hu/resource-defaults :cache-control "max-age=60")

    :etag (hu/etag 6)

    :handle-ok (render-service-document version)))

;; ---- Basis-T ---------------------------------------------------------------

(defnk render-basis-t [db [:request path-for]]
  {:data
   {:basis-t (d/basis-t db)}
   :links
   {:up {:href (path-for :service-document-handler)}
    :self {:href (path-for :basis-t-handler)}}})

(def basis-t-handler
  (resource
    (hu/resource-defaults)

    :handle-ok render-basis-t))

;; ---- Health ----------------------------------------------------------------

(defnk render-health [search-conn [:request path-for]]
  {:data
   {:search
    (let [index-exists? (<?? (sapi/index-exists? search-conn))]
      (cond-> {:cluster-status (:status (<?? (sapi/cluster-health search-conn)))
               :index-exists? index-exists?}
        index-exists?
        (assoc :num-docs (-> (<?? (sapi/index-status search-conn))
                             :indices :lens :docs :num_docs))))}
   :links
   {:up {:href (path-for :service-document-handler)}
    :self {:href (path-for :health-handler)}}})

(def health-handler
  (resource
    (hu/resource-defaults)

    :handle-ok render-health))

(defn item-code-list-item-count-handler [path-for]
  (resource
    (hu/resource-defaults)

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
      (hu/error-body path-for (str "Item " id " has no code list item with "
                                   "code " code ".")))))

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
    (hu/resource-defaults)

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
    (hu/error-body path-for "Code List not found.")))

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
    (hu/resource-defaults)

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
    (hu/resource-defaults)

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
    (hu/resource-defaults)

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
    (hu/error-body path-for "No snapshot found.")))

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
    (hu/resource-defaults :cache-control "max-age=3600")

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

    :etag (hu/etag #(-> % :snapshot :tx-id) 1)

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
   :basis-t-handler basis-t-handler
   :health-handler health-handler

   :all-studies-handler study/all-studies-handler
   :find-study-handler study/find-handler
   :study-handler study/handler
   :create-study-handler study/create-handler
   :study-profile-handler (hu/profile-handler :study study/schema)

   :study-study-event-defs-handler study-event-def/list-handler
   :find-study-event-def-handler (study/find-child-handler :study-event-def)
   :study-event-def-handler study-event-def/handler
   :create-study-event-def-handler study-event-def/create-handler
   :study-event-def-profile-handler (hu/profile-handler :study-event-def study-event-def/schema)

   :form-refs-handler form-ref/list-handler
   :find-form-ref-handler form-ref/find-handler
   :form-ref-handler form-ref/handler
   :create-form-ref-handler form-ref/create-handler

   :study-form-defs-handler form-def/list-handler
   :find-form-def-handler (study/find-child-handler :form-def)
   :form-def-handler form-def/handler
   :form-count-handler (form-def/form-def-count-handler path-for)
   :create-form-def-handler form-def/create-handler
   :form-def-profile-handler (hu/profile-handler :form-def form-def/schema)

   :item-group-refs-handler item-group-ref/list-handler
   :find-item-group-ref-handler item-group-ref/find-handler
   :item-group-ref-handler item-group-ref/handler
   :create-item-group-ref-handler item-group-ref/create-handler

   :study-item-group-defs-handler item-group-def/list-handler
   :find-item-group-def-handler (study/find-child-handler :item-group-def)
   :item-group-def-handler item-group-def/handler
   :item-group-count-handler (item-group-def/item-group-def-count-handler path-for)
   :create-item-group-def-handler item-group-def/create-handler
   :item-group-def-profile-handler (hu/profile-handler :item-group-def item-group-def/schema)

   :item-refs-handler item-ref/list-handler
   :find-item-ref-handler item-ref/find-handler
   :item-ref-handler item-ref/handler
   :create-item-ref-handler item-ref/create-handler

   :study-item-defs-handler item-def/list-handler
   :find-item-def-handler (study/find-child-handler :item-def)
   :item-def-handler item-def/handler
   :item-def-count-handler (item-def/item-def-count-handler path-for)
   :create-item-def-handler item-def/create-handler
   :item-def-profile-handler (hu/profile-handler :item-def item-def/schema)

   :item-code-list-item-count-handler
   (item-code-list-item-count-handler path-for)
   :code-list-handler (code-list-handler path-for)

   :subject-handler subject/handler
   :create-subject-handler subject/create-handler
   :delete-subject-handler subject/delete-handler

   :query-handler (query-handler path-for)
   :snapshot-handler (snapshot-handler path-for)
   :all-snapshots-handler (all-snapshots-handler path-for)
   :most-recent-snapshot-handler (most-recent-snapshot-handler path-for)

   :all-attachment-types-handler attachment-type/all-attachment-types-handler
   :attachment-type-handler attachment-type/handler
   :create-attachment-type-handler attachment-type/create-handler})
