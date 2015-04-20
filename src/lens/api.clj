(ns lens.api
  (:use plumbing.core)
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [clojure.core.cache :as cache]
            [datomic.api :as d]
            [lens.util :as util :refer [entity?]]
            [lens.k-means :refer [k-means]]))

;; ---- Last Loaded -----------------------------------------------------------

(defn last-loaded [db]
  (:db/txInstant (d/entity db (d/t->tx (d/basis-t db)))))

;; ---- Single Accessors ------------------------------------------------------

(defn subject
  "Returns the subject with the id or nil if none was found."
  [db id]
  (d/entity db [:subject/id id]))

(defn study
  "Returns the study with the id or nil if none was found."
  [db id]
  (d/entity db [:study/id id]))

(defn study-event
  "Returns the study-event with the id or nil if none was found."
  [db id]
  (d/entity db [:study-event/id id]))

(defn form
  "Returns the form with the id or nil if none was found."
  [db id]
  (d/entity db [:form/id id]))

(defn item-group
  "Returns the item group with the id or nil if none was found."
  [db id]
  (d/entity db [:item-group/id id]))

(defn item
  "Returns the item with the given ID or nil if none was found."
  [db id]
  (d/entity db [:item/id id]))

(defn code-list
  "Returns the code-list with the given ID or nil if none was found."
  [db id]
  (d/entity db [:code-list/id id]))

(def ^:private cli-cache (atom (cache/lru-cache-factory {} :threshold 1024)))

(defn- code-list-item' [db item-eid code]
  (some->> (d/q '[:find ?cli .
                  :in $ ?i ?c
                  :where
                  [?i :item/code-list ?cl]
                  [?cl :code-list/attr ?a]
                  [?cli :code-list-item/code-list ?cl]
                  [?cli ?a ?c]]
                db item-eid code)
           (d/entity db)))

(defn convert-code [item code]
  (condp = (:item/attr item)
    :data-point/long-value (if (string? code) (Long/parseLong code) code)))

(defn code-list-item
  "Returns the code-list-item with the code from the items code-list."
  [item code]
  {:pre [(entity? item)]}
  (let [code (convert-code item code)
        db (d/entity-db item)
        item-eid (:db/id item)
        key {:basis-t (d/basis-t db)
             :item-eid item-eid
             :code code}]
    (-> (util/update-cache! cli-cache key (code-list-item' db item-eid code))
        (cache/lookup key))))

;; ---- Retractors ------------------------------------------------------------

(defn retract-subject
  "Retracts the subject with the id.

  Returns true if an existing subject was retracted and false if the subject
  did not exist at the time the transaction happend."
  [conn id]
  (try
    @(d/transact-async conn [[:retract-subject id]])
    true
    (catch Exception e
      (if (= :unknown-subject (util/error-type e))
        false
        (throw e)))))

;; ---- Lists -----------------------------------------------------------------

(defn- list-all [q db & inputs]
  (->> (apply d/q q db inputs)
       (r/map #(d/entity db (first %)))))

(defn all-studies
  "Returns a reducible coll of all studies."
  [db]
  (list-all '[:find ?e :where [?e :study/id]] db))

(defn all-study-events
  "Returns a reducible coll of all study-events."
  [db]
  (list-all '[:find ?e :where [?e :study-event/id]] db))

(defn all-forms
  "Returns a reducible coll of all forms sorted by :form/id."
  [db]
  (->> (d/datoms db :avet :form/id)
       (r/map #(d/entity db (:e %)))))

;; ---- Traversal -------------------------------------------------------------

(defn forms [study]
  (:form/_studies study))

(defn studies [form]
  (:form/studies form))

(defn item-groups
  "Returns a seq of all item-groups of a form."
  [form]
  (:item-group/_form form))

(defn items
  "Returns a seq of all items of an item-group."
  [item-group]
  (:item/_item-group item-group))

(defn code-list-items
  "Returns a seq of all code-list-item of a code-list."
  [code-list]
  (:code-list-item/_code-list code-list))

(defn code [code-list-item]
  (let [code-list (:code-list-item/code-list code-list-item)]
    ((:code-list/attr code-list) code-list-item)))

;; ---- Lists Matching --------------------------------------------------------

(def matching-rules
  '[[(starts-with-ignore-case ?s ?prefix)
     [(.toLowerCase ^String ?s) ?ls]
     [(.toLowerCase ^String ?prefix) ?lprefix]
     [(.startsWith ^String ?ls ?lprefix)]]
    [(form-or-below ?e ?f)
     [?e :form/id]
     [?f :form/id]
     [(= ?e ?f)]]
    [(form-or-below ?e ?f)
     [?e :item-group/form ?f]]
    [(form-or-below ?e ?f)
     [?e :item/item-group ?ig]
     [?ig :item-group/form ?f]]
    [(form-search ?filter ?f)
     [(fulltext $ :name ?filter) [[?e _]]]
     (form-or-below ?e ?f)]
    [(form-search ?filter ?f)
     [(fulltext $ :item/question ?filter) [[?i _]]]
     [?i :item/item-group ?ig]
     [?ig :item-group/form ?f]]
    [(form-search ?filter ?f)
     [(fulltext $ :code-list-item/label ?filter) [[?cli _]]]
     [?cli :code-list-item/code-list ?cl]
     [?i :item/code-list ?cl]
     [?i :item/item-group ?ig]
     [?ig :item-group/form ?f]]
    [(form-search ?filter ?f)
     [?f :form/id ?id]
     (starts-with-ignore-case ?id ?filter)]
    [(form-search ?filter ?f)
     [?f :form/alias ?a]
     (starts-with-ignore-case ?a ?filter)]
    [(form-search ?filter ?f)
     [?ig :item-group/form ?f]
     [?i :item/item-group ?ig]
     [?i :item/id ?id]
     (starts-with-ignore-case ?id ?filter)]
    [(item-group-or-below ?e ?ig)
     [?e :item-group/form]
     [(= ?e ?ig)]]
    [(item-group-or-below ?e ?ig)
     [?e :item/item-group ?ig]]
    [(item-group-search ?filter ?f ?ig)
     [(fulltext $ :name ?filter) [[?e _]]]
     [?ig :item-group/form ?f]
     (item-group-or-below ?e ?ig)]
    [(item-group-search ?filter ?f ?ig)
     [(fulltext $ :item/question ?filter) [[?i _]]]
     [?ig :item-group/form ?f]
     [?i :item/item-group ?ig]]
    [(item-group-search ?filter ?f ?ig)
     [(fulltext $ :code-list-item/label ?filter) [[?cli _]]]
     [?ig :item-group/form ?f]
     [?i :item/item-group ?ig]
     [?i :item/code-list ?cl]
     [?cli :code-list-item/code-list ?cl]]
    [(item-group-search ?filter ?f ?ig)
     [?ig :item-group/form ?f]
     [?i :item/item-group ?ig]
     [?i :item/id ?id]
     (starts-with-ignore-case ?id ?filter)]
    [(item-search ?filter ?ig ?i)
     [(fulltext $ :name ?filter) [[?i _]]]
     [?i :item/item-group ?ig]]
    [(item-search ?filter ?ig ?i)
     [(fulltext $ :item/question ?filter) [[?i _]]]
     [?i :item/item-group ?ig]]
    [(item-search ?filter ?ig ?i)
     [(fulltext $ :code-list-item/label ?filter) [[?cli _]]]
     [?i :item/item-group ?ig]
     [?i :item/code-list ?cl]
     [?cli :code-list-item/code-list ?cl]]
    [(item-search ?filter ?ig ?i)
     [?i :item/item-group ?ig]
     [?i :item/id ?id]
     (starts-with-ignore-case ?id ?filter)]])

(defn list-matching-forms
 "Returns a seq of forms matching the filter expression sorted by :form/id."
 [db filter]
  {:pre [(string? filter)]}
  (util/timer
   {:fn 'list-matching-forms :args {:filter filter}}
   (when-not (str/blank? filter)
     (->> (d/q '[:find [?f ...] :in $ % ?filter :where (form-search ?filter ?f)]
               db matching-rules filter)
          (map #(d/entity db %))
          (sort-by :form/id)))))

(defn list-matching-item-groups
  "Returns a seq of item-groups of a form matching the filter expression sorted
  by :item-groups/rank."
  [form filter]
  {:pre [(entity? form) (string? filter)]}
  (util/timer
   {:fn 'list-matching-item-groups :args {:form (:form/id form)
                                          :filter filter}}
   (when-not (str/blank? filter)
     (let [db (d/entity-db form)]
       (->> (d/q '[:find [?ig ...] :in $ % ?f ?filter
                   :where (item-group-search ?filter ?f ?ig)]
                 db matching-rules (:db/id form) filter)
            (map #(d/entity db %))
            (sort-by :item-group/rank))))))

(defn list-matching-items
  "Returns a seq of items of an item-group matching the filter expression
  sorted by :item/rank."
  [item-group filter]
  {:pre [(entity? item-group) (string? filter)]}
  (util/timer
   {:fn 'list-matching-items :args {:item-group (:name item-group)
                                    :filter filter}}
   (when-not (str/blank? filter)
     (let [db (d/entity-db item-group)]
       (->> (d/q '[:find [?i ...] :in $ % ?ig ?filter
                   :where (item-search ?filter ?ig ?i)]
                 db matching-rules (:db/id item-group) filter)
            (map #(d/entity db %))
            (sort-by :item/rank))))))

;; ---- Subject-Stat ----------------------------------------------------------

(def ^:private subject-stat
  "Cache for subject statistics.

  Keys are maps of :basis-t and :id. Values are counts."
  (atom (cache/lru-cache-factory {} :threshold 8192)))

(defn- subject-stat-key [db id]
  {:basis-t (d/basis-t db) :id id})

(defn- num-subjects [db entity-eid]
  (-> (d/q '[:find (count ?s) .
             :in $ ?e
             :where [?e :visits ?v] [?v :visit/subject ?s]]
           db entity-eid)
      (or 0)))

(defmacro using-subject-stat! [e]
  `(let [db# (d/entity-db ~e)
         key# (subject-stat-key db# (:db/id ~e))]
     (-> subject-stat
         (util/update-cache! key# (num-subjects db# (:id key#)))
         (cache/lookup key#))))

(defn num-study-event-subjects [study-event]
  {:pre [(entity? study-event)]}
  (using-subject-stat! study-event))

(defn num-form-subjects [form]
  {:pre [(entity? form)]}
  (using-subject-stat! form))

(defn num-item-group-subjects [item-group]
  {:pre [(entity? item-group)]}
  (using-subject-stat! item-group))

(defn num-item-subjects [item]
  {:pre [(entity? item)]}
  (using-subject-stat! item))

(defn num-code-list-item-subjects [code-list-item]
  {:pre [(entity? code-list-item)]}
  (using-subject-stat! code-list-item))

;; ---- Values ----------------------------------------------------------------

(def values-rules
  '[[(item-values ?i ?v)
     [?i :item/attr ?a]
     [?dp :data-point/item ?i]
     [?dp ?a ?v]]
    [(item-values-dp ?i ?dp ?v)
     [?i :item/attr ?a]
     [?dp :data-point/item ?i]
     [?dp ?a ?v]]])

(defmacro values-query [min-max]
  `'[:find (~min-max ~'?v) . :in ~'$ ~'% ~'?i :where (~'item-values ~'?i ~'?v)])

(defn min-value [item]
  {:pre [(entity? item)]}
  (d/q (values-query min) (d/entity-db item) values-rules (:db/id item)))

(defn max-value [item]
  {:pre [(entity? item)]}
  (d/q (values-query max) (d/entity-db item) values-rules (:db/id item)))

(defn values [item]
  (d/q '[:find [?v ...] :with ?dp :in $ % ?i :where (item-values-dp ?i ?dp ?v)]
       (d/entity-db item) values-rules (:db/id item)))

(defn nearest-rank
  "Returns the nearest rank of the percentile p in a list of n items."
  [p n]
  {:pre [(<= 0 p 1) (not= 0 n)]}
  (int (Math/ceil (* p n))))

(defn value-quartiles
  "Returns nil when the item has no values."
  [item]
  {:pre [(entity? item)]}
  (when-let [values (seq (values item))]
    (let [values (->> values (sort) (vec))
          n (count values)]
      {:min (first values)
       :q1 (values (nearest-rank 0.25 n))
       :q2 (values (nearest-rank 0.5 n))
       :q3 (values (nearest-rank 0.75 n))
       :max (peek values)})))

(def ^:private histogram-stat
  "Cache for histogram statistics.

  Keys are maps of :basis-t and :id. Values are histograms."
  (atom (cache/lru-cache-factory {} :threshold 1024)))

(defn- histogram-stat-key [db id]
  {:basis-t (d/basis-t db) :id id})

(defn- value-histogram' [item]
  (when-let [values (seq (values item))]
    (let [values (->> values (sort) (vec))
          n (count values)
          init-means [(first values)
                      (values (nearest-rank 0.25 n))
                      (values (nearest-rank 0.5 n))
                      (values (nearest-rank 0.75 n))
                      (peek values)]]
      (->> (k-means init-means values)
           (map-vals count)))))

(defn value-histogram
  "Returns nil when the item has no values."
  [item]
  {:pre [(entity? item)]}
  (let [db (d/entity-db item)
        key (histogram-stat-key db (:db/id item))]
    (-> histogram-stat
        (util/update-cache! key (value-histogram' item))
        (cache/lookup key))))

;; ---- Query -----------------------------------------------------------------

(defn clean-atom [atom]
  (let [{:keys [type id] :as atom-map} (zipmap [:type :id] atom)]
    (when (and (#{:item :item-group :form :code-list-item} type)
               (not (nil? id)))
      atom-map)))

(defn clean-disjunction [disjunction]
  (->> (map clean-atom disjunction)
       (filter seq)))

(defn clean-query [query]
  (->> (map clean-disjunction query)
       (filter seq)))

(defn atom-entity [db {:keys [type id]}]
  (if (= :code-list-item type)
    (code-list-item (item db (:item-id id)) (:code id))
    (d/entity db [(keyword (name type) "id") id])))

(defn visit-stat-visits
  "Returns the set of visits which have data-points described by the given
  atom.

  The atom is a map of :type and :id."
  [db atom]
  (:visits (atom-entity db atom)))

(defn query-disjunction [db disjunction]
  (->> disjunction
       (map #(visit-stat-visits db %))
       (apply set/union)))

(defn query-conjunction [db conjuction]
  (if-let [s (seq (pmap #(query-disjunction db %) conjuction))]
    (apply set/intersection s)
    #{}))

(defn query
  "Issues a query against the database.

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

  The query returns the seq of visits which has data points satisfying the
  query."
  [db {:keys [items study-events]}]
  (let [visits (query-conjunction db (clean-query items))]
    (if (seq study-events)
      (filter #((set study-events) (:study-event/id (:visit/study-event %))) visits)
      visits)))
