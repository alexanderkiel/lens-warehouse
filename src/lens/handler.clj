(ns lens.handler
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [liberator.core :refer [resource]]
            [lens.handler.util :refer :all]
            [lens.api :as api]
            [lens.reducers :as lr]
            [clojure.string :as str]
            [lens.util :as util]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clojure.edn :as edn])
  (:import [java.net URLEncoder]
           [java.util UUID]))

(def page-size 50)

(def paginate (partial util/paginate page-size))

(defn parse-page-num [s]
  (if (and s (re-matches #"[0-9]+" s))
    (util/parse-int s)
    1))

(defn url-encode
  [string]
  (some-> string str (URLEncoder/encode "UTF-8") (.replace "+" "%20")))

(defn pr-form-data [data]
  (->> (for [[k v] data :when k]
         (str/join "=" [(name k) (url-encode v)]))
       (str/join "&")))

(defn query-map [page-num filter]
  (-> {:page-num page-num}
      (assoc-when :filter (when-not (str/blank? filter) filter))
      (pr-form-data)))

(defn render-embedded-count [self-href count]
  {:value count :links {:self {:href self-href}}})

(defn assoc-count
  "Assocs the count under :embedded :lens/count or a :len/count link with href
  if count is nil."
  [e count href]
  (if count
    (assoc-in e [:embedded :lens/count] (render-embedded-count href count))
    (assoc-in e [:links :lens/count :href] href)))

;; ---- Service Document ------------------------------------------------------

(defn service-document-handler [path-for version]
  (resource
    resource-defaults

    :handle-ok
    {:name "Lens Warehouse"
     :version version
     :links
     {:self {:href (path-for :service-document-handler)}
      :lens/all-study-events {:href (path-for :all-study-events-handler)}
      :lens/all-forms {:href (path-for :all-forms-handler)}
      :lens/all-snapshots {:href (path-for :all-snapshots-handler)}}
     :forms
     {:lens/find-form
      {:action (path-for :find-form-handler)
       :method "GET"
       :params
       {:id
        {:type :string}}}
      :lens/find-item-group
      {:action (path-for :find-item-group-handler)
       :method "GET"
       :params
       {:id
        {:type :string}}}
      :lens/find-item
      {:action (path-for :find-item-handler)
       :method "GET"
       :params
       {:id
        {:type :string}}}
      :lens/query
      {:action (path-for :query-handler)
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

(defn render-embedded-study-event [path-for study-event]
  {:id (:study-event/id study-event)
   :count (:count study-event)
   :type :study-event
   :links
   {:self {:href (path-for :study-event-handler :id
                           (:study-event/id study-event))}}})

(defn render-embedded-study-events [path-for study-events]
  (mapv #(render-embedded-study-event path-for %) study-events))

(defn all-study-events-handler [path-for]
  (resource
    resource-defaults

    :handle-ok
    (fnk [db [:request params]]
      (let [page-num (parse-page-num (:page-num params))
            study-events (into [] (api/all-study-events db))
            study-events (->> study-events
                              (map #(merge {:count (api/num-study-event-subjects
                                                     %)} %))
                              (sort-by :count)
                              (reverse))
            next-page? (not (lr/empty? (paginate (inc page-num) study-events)))
            page-link (fn [num] {:href (str (path-for :all-study-events-handler)
                                            "?" (query-map num nil))})]
        {:links
         (-> {:self {:href (page-link page-num)}
              :up {:href (path-for :service-document-handler)}}
             (assoc-when :prev (when (< 1 page-num) (page-link (dec page-num))))
             (assoc-when :next (when next-page? (page-link (inc page-num)))))
         :embedded
         {:lens/study-events
          (->> (paginate page-num study-events)
               (into [])
               (render-embedded-study-events path-for))}}))))

;; ---- Study-Event -----------------------------------------------------------

(defn study-event-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [study-event (api/study-event db id)]
        {:study-event study-event}))

    :handle-ok
    (fnk [study-event]
      {:id (:study-event/id study-event)
       :type :study-event
       :links
       {:up {:href (path-for :all-study-events-handler)}
        :self {:href (path-for :study-event-handler :id
                               (:study-event/id study-event))}}})

    :handle-not-found
    (error-body path-for "Study event not found.")))

;; ---- Subject ---------------------------------------------------------------

(defn get-subject-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [subject (api/subject db id)]
        {:subject subject}))

    :handle-ok
    (fnk [subject]
      {:id (:subject/id subject)
       :type :subject
       :links
       {:up {:href (path-for :service-document-handler)}
        :self {:href (path-for :subject-handler :id (:subject/id subject))}}})

    :handle-not-found
    (error-body path-for "Subject not found.")))

(defn delete-subject-handler [path-for]
  (fnk [conn [:params id]]
    (if (api/retract-subject conn id)
      {:status 204}
      (error path-for 404 "Subject not found."))))

;; ---- Forms -----------------------------------------------------------------

(defn search-item-groups-form [form]
  {:action (str "/forms/" (:form/id form) "/search-item-groups")
   :method "GET"
   :title (str "Search Item Groups of Form " (:form/id form))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-form [path-for timeout form]
  (-> {:id (:form/id form)
       :alias (:form/alias form)
       :name (:name form)
       :type :form
       :links
       {:self
        {:href (path-for :form-handler :id (:form/id form))}
        :lens/item-groups
        {:href (str "/forms/" (:form/id form) "/item-groups")}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form)}}
      (assoc-count
        (util/try-until timeout (api/num-form-subjects form))
        (path-for :form-count-handler :id (:form/id form)))))

(defn render-embedded-forms [path-for timeout forms]
  (r/map #(render-embedded-form path-for timeout %) forms))

(defn all-forms-handler [path-for]
  (resource
    resource-defaults

    :handle-ok
    (fnk [db [:request params]]
      (let [page-num (parse-page-num (:page-num params))
            filter (:filter params)
            forms (if (str/blank? filter)
                    (api/all-forms db)
                    (api/list-matching-forms db filter))
            next-page? (not (lr/empty? (paginate (inc page-num) forms)))
            page-link (fn [num] {:href (str (path-for :all-forms-handler) "?"
                                            (query-map num filter))})]
        {:links
         (-> {:self {:href (path-for :all-forms-handler)}
              :up {:href (path-for :service-document-handler)}}
             (assoc-when :prev (when (< 1 page-num) (page-link (dec page-num))))
             (assoc-when :next (when next-page? (page-link (inc page-num)))))
         :forms
         {:lens/filter
          {:action (path-for :all-forms-handler)
           :method "GET"
           :title "Filter Forms"
           :params
           {:filter
            {:type :string
             :description "Search query which allows Lucene syntax."}}}}
         :embedded
         {:lens/forms
          (->> (paginate page-num forms)
               (render-embedded-forms path-for (timeout 100))
               (into []))}}))))

;; ---- Form ------------------------------------------------------------------

(defn search-items-form [path-for item-group]
  {:action (path-for :search-items-handler :id (:item-group/id item-group))
   :method "GET"
   :title (str "Search Items of Item Group " (:name item-group))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-item-group [path-for timeout item-group]
  (-> {:id (:item-group/id item-group)
       :name (:name item-group)
       :type :item-group
       :links
       {:self
        {:href (path-for :item-group-handler :id (:item-group/id item-group))}}
       :forms
       {:lens/search-items (search-items-form path-for item-group)}}
      (assoc-count
        (util/try-until timeout (api/num-item-group-subjects item-group))
        (path-for :item-group-count-handler :id (:item-group/id item-group)))))

(defn render-embedded-item-groups [path-for timeout item-groups]
  (pmap #(render-embedded-item-group path-for timeout %) item-groups))

(defn form-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [form (api/form db id)]
        {:form form}))

    :handle-ok
    (fnk [form]
      {:id (:form/id form)
       :alias (:form/alias form)
       :name (:name form)
       :type :form
       :links
       {:up {:href (path-for :all-forms-handler)}
        :self {:href (path-for :form-handler :id (:form/id form))}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form)}
       :embedded
       {:lens/item-groups
        (->> (api/item-groups form)
             (sort-by :item-group/rank)
             (render-embedded-item-groups path-for (timeout 100)))}})

    :handle-not-found
    (error-body path-for "Form not found.")))

(defn find-form-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [form (api/form db id)]
        {:form form}))

    :handle-ok
    (fnk [form]
      {:id (:form/id form)
           :alias (:form/alias form)
           :name (:name form)
           :type :form
           :links
           {:up {:href (path-for :all-forms-handler)}
            :self {:href (path-for :form-handler :id (:form/id form))}}})

    :handle-not-found
    (error-body path-for "Form not found.")))

(defn form-count-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [form (api/form db id)]
        {:form form}))

    :handle-ok
    (fnk [form]
      {:value (api/num-form-subjects form)
       :links
       {:up {:href (path-for :form-handler :id (:form/id form))}
        :self {:href (path-for :form-count-handler :id (:form/id form))}}})

    :handle-not-found
    (error-body path-for "Form not found.")))

;; ---- Search Item-Groups ----------------------------------------------------

(defn search-item-groups-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [form (api/form db id)]
        {:form form}))

    :processable?
    (fnk [[:request params]]
      (not (str/blank? (:query params))))

    :handle-ok
    (fnk [form [:request [:params query]]]
      {:links {:up {:href (path-for :form-handler :id (:form/id form))}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form)}
       :embedded
       {:lens/item-groups
        (->> (api/list-matching-item-groups form query)
             (render-embedded-item-groups path-for (timeout 100)))}})

    :handle-not-found
    (error-body path-for "Form not found.")))

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

(defn render-embedded-item [path-for timeout item]
  (-> {:id (:item/id item)
       :question (:item/question item)
       :type :item
       :value-type (value-type item)
       :links
       (-> {:self {:href (path-for :item-handler :id (:item/id item))}}
           (assoc-code-list-link item))}
      (assoc-when :name (:name item))
      (assoc-count
        (util/try-until timeout (api/num-item-subjects item))
        (path-for :item-count-handler :id (:item/id item)))))

(defn render-embedded-items [path-for timeout items]
  (pmap #(render-embedded-item path-for timeout %) items))

(defn item-group-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item-group (api/item-group db id)]
        {:item-group item-group}))

    :handle-ok
    (fnk [item-group]
      (let [id (:item-group/id item-group)]
        {:id id
         :name (:name item-group)
         :type :item-group
         :links
         {:up (let [form (:item-group/form item-group)]
                {:href (path-for :form-handler :id (:form/id form))
                 :title (:name form)})
          :self {:href (path-for :item-group-handler :id id)}}
         :forms
         {:lens/search-items (search-items-form path-for item-group)}
         :embedded
         {:lens/items
          (->> (api/items item-group)
               (sort-by :item/rank)
               (render-embedded-items path-for (timeout 100)))}}))

    :handle-not-found
    (error-body path-for "Item group not found.")))

(defn find-item-group-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item-group (api/item-group db id)]
        {:item-group item-group}))

    :handle-ok
    (fnk [item-group]
      (let [id (:item-group/id item-group)]
        {:id id
         :name (:name item-group)
         :type :item-group
         :links
         {:up (let [form (:item-group/form item-group)]
                {:href (path-for :form-handler :id (:form/id form))
                 :title (:name form)})
          :self {:href (path-for :item-group-handler :id id)}}}))

    :handle-not-found
    (error-body path-for "Item group not found.")))

(defn item-group-count-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item-group (api/item-group db id)]
        {:item-group item-group}))

    :handle-ok
    (fnk [item-group]
      (let [id (:item-group/id item-group)]
        {:value (api/num-item-group-subjects item-group)
         :links
         {:up {:href (path-for :item-group-handler :id id)}
          :self {:href (path-for :item-group-count-handler :id id)}}}))

    :handle-not-found
    (error-body path-for "Item group not found.")))

;; ---- Search-Items ----------------------------------------------------------

(defn search-items-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item-group (api/item-group db id)]
        {:item-group item-group}))

    :processable?
    (fnk [[:request params]]
      (not (str/blank? (:query params))))

    :handle-ok
    (fnk [item-group [:request [:params query]]]
      {:links {:up {:href (path-for :item-group-handler :id
                                    (:item-group/id item-group))}}
       :forms
       {:lens/search-items (search-items-form path-for item-group)}
       :embedded
       {:lens/items
        (->> (api/list-matching-items item-group query)
             (render-embedded-items path-for (timeout 100)))}})

    :handle-not-found
    (error-body path-for "Item group not found.")))

;; ---- Item ------------------------------------------------------------------

(defn numeric? [item]
  (#{:data-point/long-value :data-point/float-value} (:item/attr item)))

(defn render-embedded-item-code-list-item [path-for timeout item code-list-item]
  (let [id (:item/id item) code (api/code code-list-item)]
    (-> {:id {:item-id id :code code}
         :item-id (:item/id item)
         :code code
         :label (:code-list-item/label code-list-item)
         :type :code-list-item}
        (assoc-count
          (util/try-until timeout (api/num-code-list-item-subjects
                                              code-list-item))
          (path-for :item-code-list-item-count-handler :id id :code code))
        (assoc-when :item-name (:name item)))))

(defn render-embedded-item-code-list-items [path-for timeout item
                                            code-list-items]
  (map #(render-embedded-item-code-list-item path-for timeout item %)
       code-list-items))

(defn item-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item (api/item db id)]
        {:item item}))

    :handle-ok
    (fnk [item]
     (-> {:id (:item/id item)
          :type :item
          :value-type (value-type item)
          :links
          (-> {:up
               {:href (path-for :item-group-handler :id
                                (:item-group/id (:item/item-group item)))}
               :self
               {:href (path-for :item-handler :id (:item/id item))}}
              (assoc-code-list-link item))}
         (assoc-when
           :embedded
           (when-let [code-list (:item/code-list item)]
             {:lens/item-code-list-items
              (->> (api/code-list-items code-list)
                   ;; TODO: use :code-list-item/rank instead
                   (sort-by (:code-list/attr code-list))
                   (render-embedded-item-code-list-items path-for (timeout 100)
                                                         item))}))
         (assoc-when :name (:name item))
         (assoc-when :question (:item/question item))
         (assoc-when :value-histogram (when (numeric? item)
                                        (api/value-histogram item)))))

    :handle-not-found
    (error-body path-for "Item not found.")))

(defn item-count-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item (api/item db id)]
        {:item item}))

    :handle-ok
    (fnk [item]
      {:value (api/num-item-subjects item)
       :links
       {:up {:href (path-for :item-handler :id (:item/id item))}
        :self {:href (path-for :item-count-handler :id (:item/id item))}}})

    :handle-not-found
    (error-body path-for "Item not found.")))

(defn item-code-list-item-count-handler [path-for]
  (resource
    resource-defaults

    :exists?
    (fnk [db [:request [:params id code]]]
      (when-let [item (api/item db id)]
        (when-let [code-list-item (api/code-list-item item code)]
          {:item item
           :code-list-item code-list-item})))

    :handle-ok
    (fnk [item code-list-item]
      {:value (api/num-code-list-item-subjects code-list-item)
       :links
       {:up {:href (path-for :item-handler :id (:item/id item))}
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
    resource-defaults

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

(defn query-handler [path-for]
  (resource
    resource-defaults

    :processable?
    (fnk [[:request params]]
      (when-let [expr (:expr params)]
        (try
          {:expr (edn/read-string expr)}
          (catch Exception _))))

    :handle-ok
    (fnk [db expr]
      (let [visits (api/query db expr)]
        {:links {:up {:href (path-for :service-document-handler)}}
         :visit-count (count visits)
         :visit-count-by-study-event (visit-count-by-study-event visits)
         :visit-count-by-age-decade-and-sex
         (visit-count-by-age-decade-and-sex visits)
         :subject-count (subject-count visits)}))))

;; ---- Snapshots -------------------------------------------------------------

(defn render-embedded-snapshot [path-for snapshot]
  {:links
   {:self {:href (path-for :snapshot-handler :id (:tx-id snapshot))}}
   :id (str (:tx-id snapshot))
   :time (:db/txInstant snapshot)})

(defn render-embedded-snapshots [path-for snapshots]
  (mapv #(render-embedded-snapshot path-for %) snapshots))

(defn all-snapshots-handler [path-for]
  (resource
    resource-defaults

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
    resource-defaults

    :processable?
    (fnk [[:request params]]
      (some-> (:id params) (re-matches #"[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}")))

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [item (api/snapshot db (UUID/fromString id))]
        {:item item}))

    :handle-ok
    (fnk [snapshot]
      {:links
       {:self {:href (path-for :snapshot-handler :id (:tx-id snapshot))}}
       :id (str (:tx-id snapshot))
       :time (:db/txInstant snapshot)})))

;; ---- Handlers --------------------------------------------------------------

(defnk handlers [path-for version]
  {:service-document-handler (service-document-handler path-for version)
   :all-study-events-handler (all-study-events-handler path-for)
   :study-event-handler (study-event-handler path-for)
   :get-subject-handler (get-subject-handler path-for)
   :delete-subject-handler (delete-subject-handler path-for)
   :all-forms-handler (all-forms-handler path-for)
   :find-form-handler (find-form-handler path-for)
   :form-handler (form-handler path-for)
   :form-count-handler (form-count-handler path-for)
   :search-item-groups-handler (search-item-groups-handler path-for)
   :find-item-group-handler (find-item-group-handler path-for)
   :item-group-handler (item-group-handler path-for)
   :item-group-count-handler (item-group-count-handler path-for)
   :search-items-handler (search-items-handler path-for)
   :find-item-handler (item-handler path-for)
   :item-handler (item-handler path-for)
   :item-count-handler (item-count-handler path-for)
   :item-code-list-item-count-handler
   (item-code-list-item-count-handler path-for)
   :code-list-handler (code-list-handler path-for)
   :query-handler (query-handler path-for)
   :snapshot-handler (snapshot-handler path-for)
   :all-snapshots-handler (all-snapshots-handler path-for)})
