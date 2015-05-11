(ns lens.routes
  (:use plumbing.core)
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [compojure.core :as compojure :refer [GET DELETE]]
            [ring.util.response :as ring-resp]
            [cemerick.url :as url]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [lens.reducers :as lr]
            [lens.util :as util]
            [lens.api :as api]
            [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r])
  (:import [java.util UUID]))

(def page-size 50)

(def paginate (partial util/paginate page-size))

(defn query-map [page-num filter]
  (-> {:page-num page-num}
      (assoc-when :filter (when-not (str/blank? filter) filter))
      (url/map->query)))

;; ---- Service Document ------------------------------------------------------

(defn service-document
  "Last-loaded is the timestamp of the last transaction on the main database.
  It should be print to #inst."
  [version last-loaded]
  (ring-resp/response
   {:name "Lens Warehouse"
    :version version
    :last-loaded last-loaded
    :links
    {:self {:href "/"}
     :lens/all-study-events {:href "/study-events"}
     :lens/all-forms {:href "/forms"}
     :lens/all-snapshots {:href "/snapshots"}}
    :forms
    {:lens/find-form
     {:action "/find-form"
      :method "GET"
      :params
      {:id
       {:type :string}}}
     :lens/find-item-group
     {:action "/find-item-group"
      :method "GET"
      :params
      {:id
       {:type :string}}}
     :lens/find-item
     {:action "/find-item"
      :method "GET"
      :params
      {:id
       {:type :string}}}
     :lens/query
     {:action "/query"
      :method "GET"
      :title "Query"
      :params
      {:expr
       {:type :string
        :description "Issues a query against the database.

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
query."}}}}}))

;; ---- Study Events ----------------------------------------------------------

(defn render-embedded-study-event [study-event]
  {:id (:study-event/id study-event)
   :count (:count study-event)
   :type :study-event
   :links
   {:self {:href (str "/study-events/" (:study-event/id study-event))}}})

(defn render-embedded-study-events [study-events]
  (mapv render-embedded-study-event study-events))

(defn study-events [db page-num]
  (let [study-events (into [] (api/all-study-events db))
        study-events (->> study-events
                          (map #(merge {:count (api/num-study-event-subjects %)} %))
                          (sort-by :count)
                          (reverse))
        next-page? (not (lr/empty? (paginate (inc page-num) study-events)))
        page-link (fn [num] {:href (str "/study-events?" (query-map num nil))})]
    (ring-resp/response
     {:links
      (-> {:self {:href "/study-events"}
           :up {:href "/"}}
          (assoc-when :prev (when (< 1 page-num) (page-link (dec page-num))))
          (assoc-when :next (when next-page? (page-link (inc page-num)))))
      :embedded
      {:lens/study-events
       (->> (paginate page-num study-events)
            (into [])
            (render-embedded-study-events))}})))

;; ---- Subject ---------------------------------------------------------------

(defn subject [db id]
  (if-let [subject (api/subject db id)]
    (ring-resp/response
     {:id (:subject/id subject)
      :type :subject
      :links
      {:up {:href "/"}
       :self {:href (str "/subjects/" id)}}})
    (ring-resp/not-found
     {:links {:up {:href "/"}}
      :error "Subject not found."})))

(defn retract-subject [conn id]
  (if (api/retract-subject conn id)
    (ring-resp/response
      {:links
       {:up {:href "/"}}})
    (ring-resp/not-found
     {:links {:up {:href "/"}}
      :error "Subject not found."})))

;; ---- Forms -----------------------------------------------------------------

(defn search-item-groups-form [form]
  {:action (str "/forms/" (:form/id form) "/search-item-groups")
   :method "GET"
   :title (str "Search Item Groups of Form " (:form/id form))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-count [self-href count]
  {:value count :links {:self {:href self-href}}})

(defn assoc-count [e href count]
  (if count
    (assoc-in e [:embedded :lens/count] (render-embedded-count href count))
    (assoc-in e [:links :lens/count :href] href)))

(defn render-embedded-form [timeout form]
  (-> {:id (:form/id form)
       :alias (:form/alias form)
       :name (:name form)
       :type :form
       :links
       {:self
        {:href (str "/forms/" (:form/id form))}
        :lens/item-groups
        {:href (str "/forms/" (:form/id form) "/item-groups")}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form)}}
      (assoc-count
        (str "/forms/" (:form/id form) "/count")
        (util/try-until timeout (api/num-form-subjects form)))))

(defn render-embedded-forms [timeout forms]
  (r/map #(render-embedded-form timeout %) forms))

(defn forms [db filter page-num]
  (let [forms (if (str/blank? filter)
                (api/all-forms db)
                (api/list-matching-forms db filter))
        next-page? (not (lr/empty? (paginate (inc page-num) forms)))
        page-link (fn [num] {:href (str "/forms?" (query-map num filter))})]
    (ring-resp/response
      {:links
       (-> {:self {:href "/forms"}
            :up {:href "/"}}
           (assoc-when :prev (when (< 1 page-num) (page-link (dec page-num))))
           (assoc-when :next (when next-page? (page-link (inc page-num)))))
       :forms
       {:lens/filter
        {:action "/forms"
         :method "GET"
         :title "Filter Forms"
         :params
         {:filter
          {:type :string
           :description "Search query which allows Lucene syntax."}}}}
       :embedded
       {:lens/forms
        (->> (paginate page-num forms)
             (render-embedded-forms (timeout 100))
             (into []))}})))

;; ---- Form ------------------------------------------------------------------

(defn search-items-form [item-group]
  {:action (str "/item-groups/" (:item-group/id item-group) "/search-items")
   :method "GET"
   :title (str "Search Items of Item Group " (:name item-group))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-item-group [timeout item-group]
  (-> {:id (:item-group/id item-group)
       :name (:name item-group)
       :type :item-group
       :links
       {:self
        {:href (str "/item-groups/" (:item-group/id item-group))}}
       :forms
       {:lens/search-items (search-items-form item-group)}}
      (assoc-count
        (str "/item-groups/" (:item-group/id item-group) "/count")
        (util/try-until timeout (api/num-item-group-subjects item-group)))))

(defn render-embedded-item-groups [timeout item-groups]
  (pmap #(render-embedded-item-group timeout %) item-groups))

(defn form [db id]
  (if-let [form (api/form db id)]
    (ring-resp/response
     {:id (:form/id form)
      :alias (:form/alias form)
      :name (:name form)
      :type :form
      :links
      {:up {:href "/forms"}
       :self {:href (str "/forms/" id)}}
      :forms
      {:lens/search-item-groups (search-item-groups-form form)}
      :embedded
      {:lens/item-groups
       (->> (api/item-groups form)
            (sort-by :item-group/rank)
            (render-embedded-item-groups (timeout 100)))}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Form not found."})))

(defn form-bare [db id]
  (if-let [form (api/form db id)]
    (ring-resp/response
     {:id (:form/id form)
      :alias (:form/alias form)
      :name (:name form)
      :type :form
      :links
      {:up {:href "/forms"}
       :self {:href (str "/forms/" id)}}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Form not found."})))

(defn form-count [db id]
  (if-let [form (api/form db id)]
    (ring-resp/response
     {:value (api/num-form-subjects form)
      :links
      {:up {:href (str "/forms/" id)}
       :self {:href (str "/forms/" id "/count")}}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Form not found."})))

;; ---- Search Item-Groups ----------------------------------------------------

(defn search-item-groups [db form-id query]
  (if-let [form (api/form db form-id)]
    (ring-resp/response
     {:links {:up {:href (str "/forms/" form-id)}}
      :forms
      {:lens/search-item-groups (search-item-groups-form form)}
      :embedded
      {:lens/item-groups
       (->> (api/list-matching-item-groups form query)
            (render-embedded-item-groups (timeout 100)))}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Form not found."})))

;; ---- Item-Group ------------------------------------------------------------

(defn value-type [item]
  (->> (:item/attr item)
       ({:data-point/float-value :number
         :data-point/long-value :number
         :data-point/instant-value :date
         :data-point/string-value :string})))

(defn code-list-link [code-list]
  (-> {:href (str "/code-lists/" (:code-list/id code-list))}
      (assoc-when :title (:name code-list))))

(defn assoc-code-list-link [m item]
  (assoc-when m :lens/code-list
                (some-> (:item/code-list item) (code-list-link))))

(defn render-embedded-item [timeout item]
  (-> {:id (:item/id item)
       :question (:item/question item)
       :type :item
       :value-type (value-type item)
       :links
       (-> {:self {:href (str "/items/" (:item/id item))}}
           (assoc-code-list-link item))}
      (assoc-when :name (:name item))
      (assoc-count
        (str "/items/" (:item/id item) "/count")
        (util/try-until timeout (api/num-item-subjects item)))))

(defn render-embedded-items [timeout items]
  (pmap #(render-embedded-item timeout %) items))

(defn item-group [db id]
  (if-let [item-group (api/item-group db id)]
    (ring-resp/response
     {:id (:item-group/id item-group)
      :name (:name item-group)
      :type :item-group
      :links
      {:up {:href (str "/forms/" (:form/id (:item-group/form item-group)))
            :title (:name (:item-group/form item-group))}
       :self {:href (str "/item-groups/" id)}}
      :forms
      {:lens/search-items (search-items-form item-group)}
      :embedded
      {:lens/items
       (->> (api/items item-group)
            (sort-by :item/rank)
            (render-embedded-items (timeout 100)))}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Item group not found."})))

(defn item-group-bare [db id]
  (if-let [item-group (api/item-group db id)]
    (ring-resp/response
     {:id (:item-group/id item-group)
      :name (:name item-group)
      :type :item-group
      :links
      {:up {:href (str "/forms/" (:form/id (:item-group/form item-group)))
            :title (:name (:item-group/form item-group))}
       :self {:href (str "/item-groups/" id)}}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Item group not found."})))

(defn item-group-count [db id]
  (if-let [item-group (api/item-group db id)]
    (ring-resp/response
     {:value (api/num-item-group-subjects item-group)
      :links
      {:up {:href (str "/item-groups/" id)}
       :self {:href (str "/item-groups/" id "/count")}}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Item group not found."})))

;; ---- Search-Items ----------------------------------------------------------

(defn search-items [db item-group-id query]
  (if-let [item-group (api/item-group db item-group-id)]
    (ring-resp/response
     {:links
      {:up {:href (str "/item-groups/" item-group-id)}}
      :forms
      {:lens/search-items (search-items-form item-group)}
      :embedded
      {:lens/items
       (->> (api/list-matching-items item-group query)
            (render-embedded-items (timeout 100)))}})
      (ring-resp/not-found
       {:links {:up {:href "/forms"}}
        :error "Item group not found."})))

;; ---- Item ------------------------------------------------------------------

(defn numeric? [item]
  (#{:data-point/long-value :data-point/float-value} (:item/attr item)))

(defn render-embedded-item-code-list-item [timeout item code-list-item]
  (-> {:id {:item-id (:item/id item) :code (api/code code-list-item)}
       :item-id (:item/id item)
       :code (api/code code-list-item)
       :label (:code-list-item/label code-list-item)
       :type :code-list-item}
      (assoc-count
        (let [id (:item/id item) code (api/code code-list-item)]
          (str "/items/" id "/code-list-item/" code "/count"))
        (util/try-until timeout (api/num-code-list-item-subjects code-list-item)))
      (assoc-when :item-name (:name item))))

(defn render-embedded-item-code-list-items [timeout item code-list-items]
  (map #(render-embedded-item-code-list-item timeout item %) code-list-items))

(defn item [db id]
  (if-let [item (api/item db id)]
    (ring-resp/response
     (-> {:id (:item/id item)
          :type :item
          :value-type (value-type item)
          :links
          (-> {:up
               {:href (str "/item-groups/"
                           (:item-group/id (:item/item-group item)))}
               :self
               {:href (str "/items/" (:item/id item))}}
              (assoc-code-list-link item))}
         (assoc-when
           :embedded
           (when-let [code-list (:item/code-list item)]
             {:lens/item-code-list-items
              (->> (api/code-list-items code-list)
                   ;; TODO: use :code-list-item/rank instead
                   (sort-by (:code-list/attr code-list))
                   (render-embedded-item-code-list-items (timeout 100) item))}))
         (assoc-when :name (:name item))
         (assoc-when :question (:item/question item))
         (assoc-when :value-histogram (when (numeric? item)
                                        (api/value-histogram item)))))
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Item not found."})))

(defn item-count [db id]
  (if-let [item (api/item db id)]
    (ring-resp/response
      {:value (api/num-item-subjects item)
       :links
       {:up {:href (str "/items/" id)}
        :self {:href (str "/items/" id "/count")}}})
    (ring-resp/not-found
     {:links {:up {:href "/forms"}}
      :error "Item not found."})))

(defn item-code-list-item-count [db id code]
  (if-let [code-list-item (some-> (api/item db id) (api/code-list-item code))]
    (ring-resp/response
      {:value (api/num-code-list-item-subjects code-list-item)
       :links
       {:up {:href (str "/items/" id)}
        :self {:href (str "/items/" id "/code-list-item/" code "/count")}}})
    (ring-resp/not-found
      {:links {:up {:href "/forms"}}
       :error (str "Item " id " has no code list item with code " code ".")})))

;; ---- Code-List -------------------------------------------------------------

(defn render-embedded-code-list-item [code-list-item]
  {:code (api/code code-list-item)
   :label (:code-list-item/label code-list-item)
   :count "?"
   :type :code-list-item})

(defn render-embedded-code-list-items [code-list-items]
  (mapv render-embedded-code-list-item code-list-items))

(defn code-list [db id]
  (if-let [code-list (api/code-list db id)]
    (ring-resp/response
     (-> {:id (:code-list/id code-list)
          :type :code-list
          :links
          {:up {:href "/"}
           :self {:href (str "/code-lists/" (:code-list/id code-list))}}
          :embedded
          {:lens/code-list-items
           (->> (api/code-list-items code-list)
                ;; TODO: use :code-list-item/rank instead
                (sort-by (:code-list/attr code-list))
                (render-embedded-code-list-items))}}
         (assoc-when :name (:name code-list))))
    (ring-resp/not-found
     {:links {:up {:href "/"}}
      :error "Code list not found."})))

;; ---- Query -----------------------------------------------------------------

(defn visit-count-by-study-event [visits]
  (->> (group-by :visit/study-event visits)
       (map-keys :study-event/id)
       (map-vals count)))

(defn age-at-visit [visit]
  (when-let [birth-date (-> visit :visit/subject :subject/birth-date)]
    (when-let [edat (:visit/edat visit)]
      (if (t/after? (c/from-date edat) (c/from-date birth-date))
        (t/in-years (t/interval (c/from-date birth-date) (c/from-date edat)))
        (- (t/in-years (t/interval (c/from-date edat) (c/from-date birth-date))))))))

(defn sex [visit]
  (-> visit :visit/subject :subject/sex name keyword))

(defn age-decade [age]
  {:pre [age]}
  (* (quot age 10) 10))

(defn visit-count-by-age-decade-and-sex [visits]
  (->> (group-by #(some-> (age-at-visit %) age-decade) visits)
       (reduce-kv #(if %2 (assoc %1 %2 %3) %1) {})
       (map-vals #(->> (group-by sex %)
                       (map-vals count)))))

(defn subject-count [visits]
  (->> (r/map :visit/subject visits)
       (into #{})
       (count)))

(defn query [db expr]
  (let [visits (api/query db (edn/read-string expr))]
    (ring-resp/response
      {:links {:up {:href "/"}}
       :visit-count (count visits)
       :visit-count-by-study-event (visit-count-by-study-event visits)
       :visit-count-by-age-decade-and-sex
       (visit-count-by-age-decade-and-sex visits)
       :subject-count (subject-count visits)})))

;; ---- Snapshots -------------------------------------------------------------

(defn render-embedded-snapshot [snapshot]
  {:links
   {:self {:href (str "/snapshots/" (:tx-id snapshot))}}
   :id (str (:tx-id snapshot))
   :time (:db/txInstant snapshot)})

(defn render-embedded-snapshots [snapshots]
  (mapv render-embedded-snapshot snapshots))

(defn snapshots [db]
  (let [snapshots (->> (api/all-snapshots db)
                       (into [])
                       (sort-by :db/txInstant)
                       (reverse))]
    (ring-resp/response
      {:links
       {:self {:href "/snapshots"}
        :up {:href "/"}}
       :embedded
       {:lens/snapshots
        (render-embedded-snapshots snapshots)}})))

(defn snapshot [db id]
  (let [snapshot (api/snapshot db (UUID/fromString id))]
    (ring-resp/response
      {:links
         {:self {:href (str "/snapshots/" (:tx-id snapshot))}}
         :id (str (:tx-id snapshot))
         :time (:db/txInstant snapshot)})))

;; ---- Routes ----------------------------------------------------------------

(defn to-int [s default]
  (if (and s (re-matches #"[0-9]+" s))
    (Integer/parseInt s)
    default))

(defn routes [version]
  (compojure/routes
   (GET "/" [db] (service-document version (api/last-loaded db)))

   (GET "/study-events" [db page-num]
        (study-events db (to-int page-num 1)))

   (GET "/subjects/:id" [db id] (subject db id))

   (DELETE "/subjects/:id" [conn id] (retract-subject conn id))

   (GET "/forms" [db filter page-num]
        (forms db filter (to-int page-num 1)))

   (GET "/forms/:id" [db id] (form db id))

   (GET "/find-form" [db id] (form-bare db id))

   (GET "/forms/:id/count" [db id] (form-count db id))

   (GET "/forms/:id/search-item-groups" [db id query]
         (search-item-groups db id query))

   (GET "/item-groups/:id" [db id] (item-group db id))

   (GET "/find-item-group" [db id] (item-group-bare db id))

   (GET "/item-groups/:id/count" [db id] (item-group-count db id))

   (GET "/item-groups/:id/search-items" [db id query]
         (search-items db id query))

   (GET "/items/:id" [db id] (item db id))

   (GET "/find-item" [db id] (item db id))

   (GET "/items/:id/count" [db id] (item-count db id))

   (GET "/items/:id/code-list-item/:code/count" [db id code]
        (item-code-list-item-count db id code))

   (GET "/code-lists/:id" [db id] (code-list db id))

   (GET "/query" [db expr] (query db expr))

   (GET "/snapshots" [db] (snapshots db))

   (GET "/snapshots/:id" [db id] (snapshot db id))

   (fn [_] (ring-resp/not-found
             {:links {:up {:href "/"}}
              :error "Resource not found."}))))
