(ns lens.handler
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [liberator.core :refer [resource to-location]]
            [pandect.algo.md5 :refer [md5]]
            [lens.handler.util :refer :all]
            [lens.api :as api]
            [lens.reducers :as lr]
            [clojure.string :as str]
            [lens.util :as util]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.edn :as edn]
            [datomic.api :as d]
            [cemerick.url :refer [url url-encode url-decode]])
  (:import [java.util UUID]))

(def page-size 50)

(def paginate (partial util/paginate page-size))

(defn parse-page-num [s]
  (if (and s (re-matches #"[0-9]+" s))
    (util/parse-long s)
    1))

(defn- assoc-filter [path filter]
  (if filter
    (str (assoc (url path) :query {:filter (url-encode filter)}))
    path))

(defn- assoc-prev [m page-num path-fn]
  (assoc-when m :prev (when (< 1 page-num) (path-fn (dec page-num)))))

(defn- assoc-next [m next-page? page-num path-fn]
  (assoc-when m :next (when next-page? (path-fn (inc page-num)))))

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

(defn- all-studies-path
  ([path-for] (all-studies-path path-for 1))
  ([path-for page-num] (path-for :all-studies-handler :page-num page-num)))

(defn service-document-handler [path-for version]
  (resource
    (resource-defaults :cache-control "max-age=60")

    :etag
    (fnk [[:representation media-type]]
      (md5 (str media-type
                (path-for :service-document-handler)
                (all-studies-path path-for)
                (path-for :all-snapshots-handler)
                (path-for :find-item-handler)
                (path-for :most-recent-snapshot-handler)
                (path-for :create-subject-handler)
                (path-for :create-study-handler)
                (path-for :find-study-handler))))

    :handle-ok
    {:name "Lens Warehouse"
     :version version
     :links
     {:self {:href (path-for :service-document-handler)}
      :lens/all-studies {:href (all-studies-path path-for)}
      :lens/all-snapshots {:href (path-for :all-snapshots-handler)}
      :lens/most-recent-snapshot {:href (path-for :most-recent-snapshot-handler)}}
     :forms
     {:lens/find-study
      {:action (path-for :find-study-handler)
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
      :lens/create-study
      {:action (path-for :create-study-handler)
       :method "POST"
       :params
       {:id
        {:type :string}
        :name
        {:type :string}
        :description
        {:type :string}}}}}))

;; ---- Study -----------------------------------------------------------------

(defn- study-path [path-for study]
  (path-for :study-handler :study-id (:study/id study)))

(defn- study-link [path-for study]
  {:href (study-path path-for study) :title (:name study)})

(defn- study-form-defs-path
  ([path-for study] (study-form-defs-path path-for study 1))
  ([path-for study page-num]
   (path-for :study-form-defs-handler :study-id (:study/id study)
             :page-num page-num)))

(defn- find-form-def-path [path-for study]
  (path-for :find-form-def-handler :study-id (:study/id study)))

(defn- create-form-def-path [path-for study]
  (path-for :create-form-def-handler :study-id (:study/id study)))

(defn- study-item-group-defs-path
  ([path-for study] (study-item-group-defs-path path-for study 1))
  ([path-for study page-num]
   (path-for :study-item-group-defs-handler :study-id (:study/id study)
             :page-num page-num)))

(defn- find-item-group-def-path [path-for study]
  (path-for :find-item-group-def-handler :study-id (:study/id study)))

(defn- create-item-group-def-path [path-for study]
  (path-for :create-item-group-def-handler :study-id (:study/id study)))

(defn- create-subject-path [path-for study]
  (path-for :create-subject-handler :study-id (:study/id study)))

(def find-study-handler
  (resource
    (standard-redirect-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (:id params))

    :location
    (fnk [[:request path-for [:params id]]]
      (study-path path-for {:study/id id}))))

(defnk exists-study? [db [:request [:params study-id]] :as ctx]
  (when-let [study (api/find-study db study-id)]
    (assoc ctx :study study)))

(def study-handler
  "Handler for GET and PUT on a study.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the study taken from a database outside of the
  transaction. The update transaction is than tried with name and description
  from that possibly old study as reference. The transaction only succeeds if
  the name and description are still the same on the in-transaction study."
  (resource
    (standard-entity-resource-defaults)

    :exists? exists-study?

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (let [study (:study ctx)]
          (md5 (str (:media-type representation)
                    (all-studies-path path-for)
                    (study-path path-for study)
                    (study-form-defs-path path-for study)
                    (study-item-group-defs-path path-for study)
                    (find-form-def-path path-for study)
                    (create-form-def-path path-for study)
                    (find-item-group-def-path path-for study)
                    (create-item-group-def-path path-for study)
                    (create-subject-path path-for study)
                    (:name study)
                    (:description study))))))

    :put!
    (fnk [conn study new-entity]
      (letfn [(select-props [study] (select-keys study [:name :description]))]
        {:update-error (api/update-study conn (:study/id study)
                                         (select-props study)
                                         (select-props new-entity))}))

    :handle-ok
    (fnk [study [:request path-for]]
      (-> {:id (:study/id study)
           :type :study
           :name (:name study)}
          (assoc-when :description (:description study))
          (assoc
            :links
            {:up {:href (all-studies-path path-for)}
             :self {:href (study-path path-for study)}
             :lens/form-defs
             {:href (study-form-defs-path path-for study)}
             :lens/item-group-defs
             {:href (study-item-group-defs-path path-for study)}}
            :forms
            {:lens/find-form-def
             {:action (find-form-def-path path-for study)
              :method "GET"
              :params
              {:id
               {:type :string}}}
             :lens/create-form-def
             {:action (create-form-def-path path-for study)
              :method "POST"
              :params
              {:id
               {:type :string}
               :name
               {:type :string}
               :description
               {:type :string}}}
             :lens/find-item-group-def
             {:action (find-item-group-def-path path-for study)
              :method "GET"
              :params
              {:id
               {:type :string}}}
             :lens/create-item-group-def
             {:action (create-item-group-def-path path-for study)
              :method "POST"
              :params
              {:id
               {:type :string}
               :name
               {:type :string}
               :description
               {:type :string}}}
             :lens/create-subject
             {:action (create-subject-path path-for study)
              :method "POST"
              :params
              {:id
               {:type :string}}}})))))

(defn render-embedded-study [path-for study]
  (-> {:id (:study/id study)
       :type :study
       :name (:name study)
       :links
       {:self {:href (study-path path-for study)}}}
      (assoc-when :description (:description study))))

(defn render-embedded-studies [path-for studies]
  (mapv #(render-embedded-study path-for %) studies))

(defn all-studies-handler [path-for]
  (resource
    (resource-defaults)

    :handle-ok
    (fnk [db [:request params]]
      (let [page-num (parse-page-num (:page-num params))
            filter (:filter params)
            studies (if (str/blank? filter)
                      (api/all-studies db)
                      (api/list-matching-studies db filter))
            next-page? (not (lr/empty? (paginate (inc page-num) studies)))
            path #(-> (all-studies-path path-for %)
                      (assoc-filter filter))]
        {:links
         (-> {:self {:href (path page-num)}
              :up {:href (path-for :service-document-handler)}}
             (assoc-prev page-num path)
             (assoc-next next-page? page-num path))
         :embedded
         {:lens/studies
          (->> (paginate page-num studies)
               (into [])
               (render-embedded-studies path-for))}}))))

(defn create-study-handler [path-for]
  (resource
    (resource-defaults)

    :allowed-methods [:post]

    :processable?
    (fnk [[:request params]]
      (and (:id params) (:name params)))

    :post!
    (fnk [conn [:request params]]
      (let [opts (select-keys params [:description])]
        (if-let [study (api/create-study conn (:id params) (:name params) opts)]
          {:study study}
          (throw (ex-info "Duplicate!" {:type ::duplicate})))))

    :location
    (fnk [study] (study-path path-for study))

    :handle-exception
    (fnk [exception]
      (if (= ::duplicate (util/error-type exception))
        (error path-for 409 "Study exists already.")
        (throw exception)))))

(defn study-child-list-resource-defaults []
  (assoc
    (resource-defaults)

    :processable?
    (fnk [[:request params]]
      (:study-id params))

    :exists? exists-study?))

;; ---- Study Event Defs ------------------------------------------------------

(defn render-embedded-study-event [path-for study-event]
  {:id (:study-event/id study-event)
   :count (:count study-event)
   :type :study-event-def
   :links
   {:self {:href (path-for :study-event-def-handler :id
                           (:study-event/id study-event))}}})

(defn render-embedded-study-event-defs [path-for study-event-defs]
  (mapv #(render-embedded-study-event path-for %) study-event-defs))

;; ---- Study Event Def -------------------------------------------------------

(defn study-event-def-handler [path-for]
  (resource
    (resource-defaults)

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [study-event (api/study-event db id)]
        {:study-event study-event}))

    :handle-ok
    (fnk [study-event]
      {:id (:study-event/id study-event)
       :type :study-event
       :links
       {:up {:href (path-for :study-event-defs-handler)}
        :self {:href (path-for :study-event-def-handler :id
                               (:study-event/id study-event))}}})

    :handle-not-found
    (error-body path-for "Study event not found.")))

;; ---- Form Defs -------------------------------------------------------------

(defn- form-def-path
  ([path-for form-def]
   (form-def-path path-for (:study/id (:study/_form-defs form-def))
                  (:form-def/id form-def)))
  ([path-for study-id form-def-id]
   (path-for :form-def-handler :study-id study-id :form-def-id form-def-id)))

(defn search-item-groups-form [form]
  {:action (str "/forms/" (:form/id form) "/search-item-groups")
   :method "GET"
   :title (str "Search Item Groups of Form " (:form/id form))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-form-def [path-for timeout form-def]
  (-> {:id (:form-def/id form-def)
       ;;TODO: alias
       :name (:name form-def)
       :links
       {:self
        {:href (form-def-path path-for form-def)}
        :lens/item-groups
        {:href (str "/forms/" (:form/id form-def) "/item-groups")}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form-def)}}
      #_(assoc-count
        (util/try-until timeout (api/num-form-subjects form-def))
        (path-for :form-count-handler :id (:form-def/id form-def)))))

(defn render-embedded-form-defs [path-for timeout forms]
  (r/map #(render-embedded-form-def path-for timeout %) forms))

(def study-form-defs-handler
  "Resource of all form-defs of a study."
  (resource
    (study-child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for params]]
      (let [page-num (parse-page-num (:page-num params))
            filter (:filter params)
            forms (if (str/blank? filter)
                    (->> (:study/form-defs study)
                         (sort-by :form-def/id))
                    (api/list-matching-form-defs study filter))
            next-page? (not (lr/empty? (paginate (inc page-num) forms)))
            path #(-> (study-form-defs-path path-for study %)
                      (assoc-filter filter))]
        {:links
         (-> {:self {:href (path page-num)}
              :up {:href (study-path path-for study)}}
             (assoc-prev page-num path)
             (assoc-next next-page? page-num path))
         :forms
         {:lens/filter
          {:action (study-form-defs-path path-for study)
           :method "GET"
           :title "Filter Form Defs"
           :params
           {:filter
            {:type :string
             :description "Search query which allows Lucene syntax."}}}}
         :embedded
         {:lens/form-defs
          (->> (paginate page-num forms)
               (render-embedded-form-defs path-for (timeout 100))
               (into []))}}))))

;; ---- Form Def --------------------------------------------------------------

(def find-form-def-handler
  (resource
    (standard-redirect-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params)))

    :location
    (fnk [[:request path-for [:params study-id id]]]
      (form-def-path path-for study-id id))))

(defnk exists-form-def? [study [:request [:params form-def-id]]]
  (when-let [form-def (api/find-form-def study form-def-id)]
    {:form-def form-def}))

(def form-def-handler
  "Handler for GET and PUT on a form-def.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the form-def taken from a database outside of the
  transaction. The update transaction is than tried with name and description
  from that possibly old form-def as reference. The transaction only succeeds if
  the name and description are still the same on the in-transaction form-def."
  (resource
    (standard-entity-resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-form-def?)))

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (let [form-def (:form-def ctx)]
          (md5 (str (:media-type representation)
                    (study-path path-for (:study/_form-defs form-def))
                    (form-def-path path-for form-def)
                    (:name form-def)
                    (:description form-def))))))

    :put!
    (fnk [conn form-def new-entity]
      (letfn [(select-props [form-def]
                            (select-keys form-def [:name :description]))]
        {:update-error (api/update-form-def conn form-def
                                            (select-props form-def)
                                            (select-props new-entity))}))

    :handle-ok
    (fnk [form-def [:request path-for]]
      (-> {:id (:form-def/id form-def)
           ;;TODO: alias
           :name (:name form-def)}
          (assoc-when :description (:description form-def))
          (assoc
            :links
            {:up (study-link path-for (:study/_form-defs form-def))
             :self {:href (form-def-path path-for form-def)}})))))

(defn form-def-count-handler [path-for]
  (resource
    (resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-form-def?)))

    :handle-ok
    (fnk [form-def]
      {:value (api/num-form-subjects form-def)
       :links
       {:up {:href (form-def-path path-for form-def)}
        :self {:href (path-for :form-count-handler :id (:form/id form-def))}}})

    :handle-not-found
    (error-body path-for "Form not found.")))

(defn create-form-def-handler [path-for]
  (resource
    (resource-defaults)

    :allowed-methods [:post]

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params) (:name params)))

    :exists? exists-study?

    :post!
    (fnk [conn study [:request params]]
      (let [opts (select-keys params [:description])]
        (if-let [form-def (api/create-form-def conn study (:id params)
                                               (:name params) opts)]
          {:form-def form-def}
          (throw (ex-info "Duplicate!" {:type ::duplicate})))))

    :location
    (fnk [form-def] (form-def-path path-for form-def))

    :handle-exception
    (fnk [exception]
      (if (= ::duplicate (util/error-type exception))
        (error path-for 409 "Form exists already.")
        (throw exception)))))

;; ---- Search Item Groups ----------------------------------------------------

(defn- item-group-def-path
  ([path-for item-group-def]
   (item-group-def-path path-for
                        (:study/id (:study/_item-group-defs item-group-def))
                        (:item-group-def/id item-group-def)))
  ([path-for study-id item-group-def-id]
   (path-for :item-group-def-handler :study-id study-id :item-group-def-id
             item-group-def-id)))

(defn search-items-form [path-for item-group]
  {:action (path-for :search-items-handler :id (:item-group/id item-group))
   :method "GET"
   :title (str "Search Items of Item Group " (:name item-group))
   :params
   {:query
    {:type :string
     :description "Search query which allows Lucene syntax."}}})

(defn render-embedded-item-group-def [path-for timeout item-group-def]
  (-> {:id (:item-group-def/id item-group-def)
       ;;TODO: alias
       :name (:name item-group-def)
       :links
       {:self
        {:href (item-group-def-path path-for item-group-def)}}
       #_:forms
       #_{:lens/search-items (search-items-form path-for item-group-def)}}
      #_(assoc-count
        (util/try-until timeout (api/num-item-group-subjects item-group-def))
        (path-for :item-group-count-handler :id (:item-group/id item-group-def)))))

(defn render-embedded-item-group-defs [path-for timeout item-groups]
  (r/map #(render-embedded-item-group-def path-for timeout %) item-groups))

(defn search-item-groups-handler [path-for]
  (resource
    (resource-defaults)

    :processable?
    (fnk [[:request params]]
      (not (str/blank? (:query params))))

    :exists?
    (fnk [db [:request [:params id]]]
      (when-let [form-def (api/find-form-def db id)]
        {:form-def form-def}))

    :handle-ok
    (fnk [form-def [:request [:params query]]]
      {:links {:up {:href (form-def-path path-for form-def)}}
       :forms
       {:lens/search-item-groups (search-item-groups-form form-def)}
       :embedded
       {:lens/item-groups
        (->> (api/list-matching-item-group-defs form-def query)
             (render-embedded-item-group-defs path-for (timeout 100)))}})

    :handle-not-found
    (error-body path-for "Form not found.")))

(def study-item-group-defs-handler
  "Resource of all item-group-defs of a study."
  (resource
    (study-child-list-resource-defaults)

    :handle-ok
    (fnk [study [:request path-for params]]
      (let [page-num (parse-page-num (:page-num params))
            filter (:filter params)
            item-groups (if (str/blank? filter)
                          (->> (:study/item-group-defs study)
                               (sort-by :item-group-def/id))
                          (api/list-matching-item-group-defs study filter))
            next-page? (not (lr/empty? (paginate (inc page-num) item-groups)))
            path #(-> (study-item-group-defs-path path-for study %)
                      (assoc-filter filter))]
        {:links
         (-> {:self {:href (path page-num)}
              :up {:href (study-path path-for study)}}
             (assoc-prev page-num path)
             (assoc-next next-page? page-num path))
         :forms
         {:lens/filter
          {:action (study-item-group-defs-path path-for study)
           :method "GET"
           :title "Filter item-group Defs"
           :params
           {:filter
            {:type :string
             :description "Search query which allows Lucene syntax."}}}}
         :embedded
         {:lens/item-group-defs
          (->> (paginate page-num item-groups)
               (render-embedded-item-group-defs path-for (timeout 100))
               (into []))}}))))

;; ---- Item Group Def --------------------------------------------------------

(def find-item-group-def-handler
  (resource
    (standard-redirect-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params)))

    :location
    (fnk [[:request path-for [:params study-id id]]]
      (item-group-def-path path-for study-id id))))

(defnk exists-item-group-def? [study [:request [:params item-group-def-id]]]
  (when-let [item-group-def (api/find-item-group-def study item-group-def-id)]
    {:item-group-def item-group-def}))

(def item-group-def-handler
  "Handler for GET and PUT on a item-group.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the item-group taken from a database outside of the
  transaction. The update transaction is than tried with name and description
  from that possibly old item-group as reference. The transaction only succeeds
  if the name and description are still the same on the in-transaction
  item-group."
  (resource
    (standard-entity-resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-item-group-def?)))

    ;;TODO: simplyfy when https://github.com/clojure-liberator/liberator/issues/219 is closed
    :etag
    (fnk [representation {status 200} [:request path-for] :as ctx]
      (when (= 200 status)
        (let [item-group-def (:item-group-def ctx)]
          (md5 (str (:media-type representation)
                    (study-path path-for (:study/_item-group-defs item-group-def))
                    (item-group-def-path path-for item-group-def)
                    (:name item-group-def)
                    (:description item-group-def))))))

    :handle-ok
    (fnk [item-group-def [:request path-for]]
      (-> {:id (:item-group-def/id item-group-def)
           ;;TODO: alias
           :name (:name item-group-def)}
          (assoc-when :description (:description item-group-def))
          (assoc
            :links
            {:up (study-link path-for (:study/_item-group-defs item-group-def))
             :self {:href (item-group-def-path path-for item-group-def)}})))))

(defn item-group-def-count-handler [path-for]
  (resource
    (resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-item-group-def?)))

    :handle-ok
    (fnk [item-group-def]
      (let [id (:item-group/id item-group-def)]
        {:value (api/num-item-group-subjects item-group-def)
         :links
         {:up {:href (item-group-def-path path-for item-group-def)}
          :self {:href (path-for :item-group-count-handler :id id)}}}))

    :handle-not-found
    (error-body path-for "Item group not found.")))

(defn create-item-group-def-handler [path-for]
  (resource
    (resource-defaults)

    :allowed-methods [:post]

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params) (:name params)))

    :exists? exists-study?

    :post!
    (fnk [conn study [:request params]]
      (let [opts (select-keys params [:description])]
        (if-let [item-group-def (api/create-item-group-def conn study (:id params)
                                                           (:name params) opts)]
          {:item-group-def item-group-def}
          (throw (ex-info "Duplicate!" {:type ::duplicate})))))

    :location
    (fnk [item-group-def] (item-group-def-path path-for item-group-def))

    :handle-exception
    (fnk [exception]
      (if (= ::duplicate (util/error-type exception))
        (error path-for 409 "item-group exists already.")
        (throw exception)))))

;; ---- Search Items ----------------------------------------------------------

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
  (r/map #(render-embedded-item path-for timeout %) items))

(defn search-items-handler [path-for]
  (resource
    (resource-defaults)

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-item-group-def?)))

    :processable?
    (fnk [[:request params]]
      (not (str/blank? (:query params))))

    :handle-ok
    (fnk [item-group-def [:request [:params query]]]
      {:links {:up {:href (item-group-def-path path-for item-group-def)}}
       :forms
       {:lens/search-items (search-items-form path-for item-group-def)}
       :embedded
       {:lens/items
        (->> (api/list-matching-items item-group-def query)
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
    (resource-defaults)

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
                {:href (item-group-def-path path-for (:item/item-group item))}
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
    (resource-defaults)

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
    (resource-defaults)

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

(defn subject-handler [path-for]
  (resource
    (resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:subject-id params)))

    :exists? (fn [ctx] (some-> (exists-study? ctx) (exists-subject?)))

    :handle-ok
    (fnk [subject]
      {:id (:subject/id subject)
       :type :subject
       :links
       {:up {:href (path-for :service-document-handler)}
        :self {:href (subject-path path-for subject)}}})

    :handle-not-found
    (error-body path-for "Subject not found.")))

(defn create-subject-handler [path-for]
  (resource
    (resource-defaults)

    :allowed-methods [:post]

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:id params)))

    :exists? exists-study?

    :post!
    (fnk [conn study [:request [:params id]]]
      (if-let [subject (api/create-subject conn study id)]
        {:subject subject}
        (throw (ex-info "" {:type ::conflict}))))

    :location
    (fnk [subject] (subject-path path-for subject))

    :handle-exception
    (fnk [exception]
      (if (= ::conflict (util/error-type exception))
        (error path-for 409 "Subject exists already.")
        (throw exception)))))

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
       :forms
       {:lens/query
        {:action (path-for :query-handler :id (:tx-id snapshot))
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
  {:service-document-handler (service-document-handler path-for version)

   :all-studies-handler (all-studies-handler path-for)
   :find-study-handler find-study-handler
   :study-handler study-handler
   :create-study-handler (create-study-handler path-for)

   :study-event-def-handler (study-event-def-handler path-for)

   :subject-handler (subject-handler path-for)
   :create-subject-handler (create-subject-handler path-for)
   :delete-subject-handler (delete-subject-handler path-for)

   :study-form-defs-handler study-form-defs-handler
   :find-form-def-handler find-form-def-handler
   :form-def-handler form-def-handler
   :form-count-handler (form-def-count-handler path-for)
   :create-form-def-handler (create-form-def-handler path-for)

   :search-item-groups-handler (search-item-groups-handler path-for)

   :study-item-group-defs-handler study-item-group-defs-handler
   :find-item-group-def-handler find-item-group-def-handler
   :item-group-def-handler item-group-def-handler
   :item-group-count-handler (item-group-def-count-handler path-for)
   :create-item-group-def-handler (create-item-group-def-handler path-for)

   :search-items-handler (search-items-handler path-for)

   :find-item-handler (item-handler path-for)
   :item-handler (item-handler path-for)
   :item-count-handler (item-count-handler path-for)
   :item-code-list-item-count-handler
   (item-code-list-item-count-handler path-for)
   :code-list-handler (code-list-handler path-for)
   :query-handler (query-handler path-for)
   :snapshot-handler (snapshot-handler path-for)
   :all-snapshots-handler (all-snapshots-handler path-for)
   :most-recent-snapshot-handler (most-recent-snapshot-handler path-for)})
