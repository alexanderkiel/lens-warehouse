(ns lens.schema
  "Functions to load the schema.

  Usage:

    (load-base-schema conn)"
  (:use plumbing.core)
  (:require [slingshot.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :refer [debug]]
            [datomic.api :as d]
            [clojure.core.reducers :as r])
  (:refer-clojure :exclude [alias]))

(defn- enum [enum]
  {:db/id (d/tempid :db.part/user)
   :db/ident enum})

(defmacro func [name doc params code]
  `{:db/id (d/tempid :db.part/user)
    :db/ident ~name
    :db/doc ~doc
    :db/fn (d/function '{:lang "clojure" :params ~params :code ~code})})

(defn- assoc-opt [opt]
  (condp = opt
    :id [:db/unique :db.unique/identity]
    :unique [:db/unique :db.unique/value]
    :index [:db/index true]
    :fulltext [:db/fulltext true]
    :many [:db/cardinality :db.cardinality/many]
    :comp [:db/isComponent true]))

(defn- assoc-opts [entity-map opts]
  (into entity-map (r/map assoc-opt opts)))

(defn- build-attr-map [entity-name def-item]
  (let [[attr type & more] def-item
        [opts doc] (if (string? (last more))
                     [(butlast more) (last more)]
                     [more nil])]
    (-> {:db/id (d/tempid :db.part/db)
         :db/ident (keyword (name entity-name) (name attr))
         :db/valueType (keyword "db.type" (name type))
         :db/cardinality :db.cardinality/one
         :db.install/_attribute :db.part/db}
        (assoc-opts opts)
        (assoc-when :db/doc doc))))

(defn- def-item-tx-builder [entity-name]
  (fn [def-item]
    (if (sequential? def-item)
      (build-attr-map entity-name def-item)
      def-item)))

(defn- build-entity-tx [tx name def]
  (into tx (r/map (def-item-tx-builder name) def)))

(defn- build-tx [entities]
  (reduce-kv build-entity-tx [] entities))

(def study
  "A clinical or epidemiological study. "
  [[:id :string :unique "The id of a study. Same as the study OID in ODM."]
   [:study-events :ref :many :comp]
   [:forms :ref :many :comp]
   [:item-groups :ref :many :comp]
   [:items :ref :many :comp]
   [:code-lists :ref :many :comp]

   (func :study.fn/create
     "Creates a study."
     [db tid id name more]
     (if-not (d/entity db [:study/id id])
       [(merge
          {:db/id tid
           :study/id id
           :name name}
          more)]
       (throw (ex-info "Duplicate." {:type :duplicate}))))

   (func :study.fn/update
     "Updates the study with the id.

     Ensures that the values in old-props are still current in the version of
     the in-transaction study."
     [db id old-props new-props]
     (if-let [study (d/entity db [:study/id id])]
       (if (= (select-keys study (keys old-props)) old-props)
         (concat (for [[prop old-val] study
                       :when (not= :study/id prop)
                       :when (nil? (prop new-props))]
                   [:db/retract (:db/id study) prop old-val])
                 (for [[prop val] new-props]
                   [:db/add (:db/id study) prop val]))
         (throw (ex-info "Conflict!" {:type :conflict})))
       (throw (ex-info "Study not found." {:type :not-found}))))])

(def study-event
  "A StudyEventDef packages a set of forms."
  [[:id :string "The id of a study-event. Unique within a study."]
   [:aliases :ref :many :comp]
   [:form-refs :ref :many :comp]

   (func :study-event.fn/create
     "Creates a study event.

     Ensures id uniquness within its study."
     [db tid study-id id name more]
     (if-let [study (d/entity db [:study/id study-id])]
       (if-not (some #{id} (-> study :study/study-events :study-event/id))
         [[:db/add (:db/id study) :study/study-events tid]
          (merge
            {:db/id tid
             :study-event/id id
             :name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))
       (throw (ex-info "Study not found." {:type :study-not-found}))))

   (func :study-event.fn/add-form
     "Adds a reference to a form to this study event.

     Ensures uniquness of forms within this study-event."
     [db study-id study-event-id form-id]
     (let [se-pred #(when (= study-event-id (:study-event/id %)) %)
           f-pred #(when (= form-id (:form/id %)) %)]
       (if-let [study (d/entity db [:study/id study-id])]
         (if-let [study-event (some se-pred (:study/study-events study))]
           (if-let [form (some f-pred (:study/forms study))]
             (let [forms (:study-event/forms study-event)]
               (if-not (some f-pred (map :form-ref/form forms))
                 (let [tid #db/id[:part/meta-data]]
                   [[:db/add (:db/id study-event) :study-event/form-refs tid]
                    {:db/id tid
                     :form-ref/form (:db/id form)
                     :form-ref/rank (inc (apply max 0 (map :form-ref/rank forms)))}])
                 (throw (ex-info "Duplicate!" {:type :duplicate}))))
             (throw (ex-info "Form not found." {:type :form-not-found})))
           (throw (ex-info "Study event not found." {:type :study-event-not-found})))
         (throw (ex-info "Study not found." {:type :study-not-found}))))
     )])

(def form-ref
  "A reference to a FormDef as it occurs within a specific StudyEventDef. The
  list of FormRefs identifies the types of forms that are allowed to occur
  within this type of study event."
  [[:form :ref]
   [:rank :long]])

(def form
  "A FormDef describes a type of form that can occur in a study."
  [[:id :string "The id of a form. Unique within a study."]
   [:aliases :ref :many :comp]
   [:item-group-refs :ref :many :comp]

   (func :form.fn/create
     "Creates a form.

     Ensures id uniquness within its study."
     [db tid study-id id name more]
     (if-let [study (d/entity db [:study/id study-id])]
       (if-not (some #{id} (-> study :study/forms :form/id))
         [[:db/add (:db/id study) :study/forms tid]
          (merge
            {:db/id tid
             :form/id id
             :name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))
       (throw (ex-info "Study not found." {:type :study-not-found}))))])

(def item-group-ref
  "A reference to a ItemGroupDef as it occurs within a specific FormDef. The
  list of ItemGroupRefs identifies the types of item groups that are allowed to
  occur within this type of form."
  [[:item-group :ref]
   [:rank :long]])

(def item-group
  "An ItemGroupDef describes a type of item group that can occur within a study."
  [[:id :string "The id of an item group. Unique within a study."]
   [:aliases :ref :many :comp]
   [:item-refs :ref :many :comp]

   (func :item-group.fn/create
     "Creates an item group.

     Ensures id uniquness within its study."
     [db tid study-id id name more]
     (if-let [study (d/entity db [:study/id study-id])]
       (if-not (some #{id} (-> study :study/item-groups :item-group/id))
         [[:db/add (:db/id study) :study/item-groups tid]
          (merge
            {:db/id tid
             :item-group/id id
             :name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))
       (throw (ex-info "Study not found." {:type :study-not-found}))))])

(def item-ref
  "A reference to an ItemDef as it occurs within a specific ItemGroupDef. The
  list of ItemRefs identifies the types of items that are allowed to occur
  within this type of item group."
  [[:item :ref]
   [:rank :long]])

(def item
  "An ItemDef describes a type of item that can occur within a study. Item
  properties include name, datatype, measurement units, range or codelist
  restrictions, and several other properties."
  [[:id :string "The id of an item. Unique within a study."]
   [:aliases :ref :many :comp]
   [:data-type :ref]
   [:question :string :fulltext]
   [:code-list :ref]

   (enum :data-type/text)
   (enum :data-type/integer)
   (enum :data-type/float)
   (enum :data-type/date)
   (enum :data-type/time)
   (enum :data-type/datetime)
   (enum :data-type/string)
   (enum :data-type/boolean)

   (func :item.fn/create
     "Creates an item.

     Ensures id uniquness within its study."
     [db tid study-id id name more]
     (if-let [study (d/entity db [:study/id study-id])]
       (if-not (some #{id} (-> study :study/items :item/id))
         [[:db/add (:db/id study) :study/items tid]
          (merge
            {:db/id tid
             :item/id id
             :name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))
       (throw (ex-info "Study not found." {:type :study-not-found}))))])

(def alias
  "An Alias provides an additional name for an element. The Context attribute
  specifies the application domain in which this additional name is relevant."
  [[:context :string]
   [:name :string]])

(def code-list
  "Defines a discrete set of permitted values for an item. The definition can be
  an explicit list of values (CodeListItem+ | EnumeratedItem+) or a reference to
  an externally defined codelist (ExternalCodeList)."
  [[:id :string "The id of a code-list. Unique within a study."]
   [:aliases :ref :many :comp]
   [:data-type :ref]

   (func :code-list.fn/create
     "Creates a code-list.

     Ensures id uniquness within its study."
     [db tid study-id id name more]
     (if-let [study (d/entity db [:study/id study-id])]
       (if-not (some #{id} (-> study :study/code-lists :code-list/id))
         [[:db/add (:db/id study) :study/code-lists tid]
          (merge
            {:db/id tid
             :code-list/id id
             :name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))
       (throw (ex-info "Study not found." {:type :study-not-found}))))])

(def subject
  {:partitions
   [{:db/ident :part/subject}]

   :attributes
   [{:db/ident :subject/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The identifier of a subject."}

    {:db/ident :subject/birth-date
     :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one
     :db/doc (str "The date of birth of a subject. One should use the 15th of "
                  "a month if there are any privacy regulations in place.")}

    {:db/ident :subject/sex
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "The sex of a subject."}]

   :enums
   [:subject.sex/male
    :subject.sex/female]

   :functions
   [(func :subject.fn/create
      "Creates a subject."
      [db tid id more]
      (if-not (d/entity db [:subject/id id])
        [(merge
           {:db/id tid
            :subject/id id}
           more)]
        (throw (ex-info (str "Subject exists already: " id)
                        {:type :subject-exists-already :id id}))))
    (func :subject.fn/retract
      "Retracts a subject including all its visits."
      [db id]
      (if-let [subject (d/entity db [:subject/id id])]
        (->> (:visit/_subject subject)
             (map #(vector :db.fn/retractEntity (:db/id %)))
             (cons [:db.fn/retractEntity (:db/id subject)]))
        (throw (ex-info (str "Unknown subject: " id)
                        {:type :unknown-subject :id id}))))]})

(def form'
  {:attributes
   [{:db/ident :form/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of a form."}

    {:db/ident :form/alias
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db/doc (str "A more meaningful but also short and technical version of "
                  "the :form/id.")}

    {:db/ident :form/studies
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/many
     :db/doc "A reference to all studies of a form."}]

   :functions
   [(func :form.fn/create
      "Creates a form."
      [db tid id name more]
      (if-not (d/entity db [:form/id id])
        [(merge
           {:db/id tid
            :form/id id
            :name name}
           more)]
        (throw (ex-info "Duplicate." {:type :duplicate}))))

    (func :form.fn/update
      "Updates the form with the id.

      Ensures that the values in old-props are still current in the version of
      the in-transaction form."
      [db id old-props new-props]
      (if-let [form (d/entity db [:form/id id])]
        (if (= (select-keys form (keys old-props)) old-props)
          (concat (for [[prop old-val] form
                        :when (not= :form/id prop)
                        :when (nil? (prop new-props))]
                    [:db/retract (:db/id form) prop old-val])
                  (for [[prop val] new-props]
                    [:db/add (:db/id form) prop val]))
          (throw (ex-info "Conflict!" {:type :conflict})))
        (throw (ex-info "Form not found." {:type :not-found}))))]})

(def base-schema
  {:partitions
   [{:db/ident :part/meta-data}
    {:db/ident :part/visit}
    {:db/ident :part/data-point}]

   :attributes
   [{:db/ident :name
     :db/valueType :db.type/string
     :db/fulltext true
     :db/cardinality :db.cardinality/one
     :db/doc "The human-readable name of some entity."}

    {:db/ident :description
     :db/valueType :db.type/string
     :db/fulltext true
     :db/cardinality :db.cardinality/one
     :db/doc "The human-readable description of some entity."}

    ;; Item

    {:db/ident :item/attr
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the data point attribute of an item."}

    ;; Code-List

    {:db/ident :code-list/attr
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the code list item attribute of a code list.
Which is one of :code-list-item/long-code or :code-list-item/string-code."}

    {:db/ident :code-list-item/code-list
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the code list of a code list item."}

    {:db/ident :code-list-item/rank
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "The rank of a code list item relative to its code list."}

    {:db/ident :code-list-item/long-code
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "The code of a code list item when its type is long."}

    {:db/ident :code-list-item/string-code
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db/doc "The code of a code list item when its type is string."}

    {:db/ident :code-list-item/label
     :db/valueType :db.type/string
     :db/fulltext true
     :db/cardinality :db.cardinality/one
     :db/doc "The label of a code list item."}

    ;; Visit
    ;;
    ;; A visit describes the actual occurence of a subject to a study event
    ;; where data points were collected.

    {:db/ident :visit/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc (str "The id of a visit which is the string concatenation of "
                  "subject id and study event id. It is only used to be able "
                  "to upsert visits.")}

    {:db/ident :visit/subject
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the subject of a visit."}

    {:db/ident :visit/study-event
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the study event of a visit."}

    {:db/ident :visit/edat
     :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one
     :db/doc "The edat of a visit."}

    ;; Data-Point

    {:db/ident :data-point/id
     :db/valueType :db.type/bytes
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of a data point."}

    {:db/ident :data-point/visit
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the visit on which a data point was collected."}

    {:db/ident :data-point/item
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the item of a data point."}

    {:db/ident :data-point/string-value
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db/doc "The value of a data point when its type is string."}

    {:db/ident :data-point/long-value
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "The value of a data point when its type is long."}

    {:db/ident :data-point/float-value
     :db/valueType :db.type/float
     :db/cardinality :db.cardinality/one
     :db/doc "The value of a data point when its type is bigint."}

    {:db/ident :data-point/instant-value
     :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one
     :db/doc "The value of a data point when its type is instant."}

    ;; Visit-Stat

    {:db/ident :visits
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/many
     :db/doc (str "All visits which have at least one data point on an "
                  "entity. The entity can be a form, item-group, item, "
                  "code-list-item or study-event.")}

    ;; Transactions
    {:db/ident :tx-id
     :db/valueType :db.type/uuid
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc (str "Marks a transaction as prepresenting a fully loaded "
                  "warehouse. Transactions missing this id represent "
                  "intermediate states of warehouse loads. Queries should only "
                  "use databases as-of transactions having this id.")}]

   :functions
   [{:db/id (d/tempid :db.part/user)
     :db/ident :add-item-group
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id form-id]
         :code [{:db/id (d/tempid :part/meta-data)
                 :item-group/id id
                 :item-group/form [:form/id form-id]}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-item
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id item-group-id attr]
         :code [{:db/id (d/tempid :part/meta-data)
                 :item/id id
                 :item/item-group [:item-group/id item-group-id]
                 :item/attr attr}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-code-list
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id code-list-attr]
         :code [{:db/id (d/tempid :part/meta-data)
                 :code-list/id id
                 :code-list/attr code-list-attr}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-code-list-to-item
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ item-id code-list-id]
         :code [[:db/add [:item/id item-id] :item/code-list
                 [:code-list/id code-list-id]]]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-code-list-item
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ code-list-id code-list-attr value]
         :code [{:db/id (d/tempid :part/meta-data)
                 :code-list-item/code-list [:code-list/id code-list-id]
                 code-list-attr value}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-to-visit-stat
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ entity-id visit-id]
         :code [[:db/add entity-id :visits visit-id]]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-visit
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [db visit-tid id subject-ref study-event-ref edat]
         :code
         [{:db/id visit-tid
           :visit/id id
           :visit/subject subject-ref
           :visit/study-event study-event-ref
           :visit/edat edat}
          [:add-to-visit-stat study-event-ref visit-tid]]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-data-point
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [db id visit-ref form-ref item-group-ref item-ref attr value]
         :code
         [{:db/id (d/tempid :part/data-point)
           :data-point/id id
           :data-point/visit visit-ref
           :data-point/item item-ref
           attr value}
          [:add-to-visit-stat form-ref visit-ref]
          [:add-to-visit-stat item-group-ref visit-ref]
          [:add-to-visit-stat item-ref visit-ref]]})}]})

(defn- assoc-tempid [m partition]
  (assoc m :db/id (d/tempid partition)))

(defn make-part
  "Assocs :db/id and :db.install/_partition to the part map."
  [part]
  (-> (assoc-tempid part :db.part/db)
      (assoc :db.install/_partition :db.part/db)))

(defn make-attr
  "Assocs :db/id and :db.install/_attribute to the attr map."
  [attr]
  (-> (assoc-tempid attr :db.part/db)
      (assoc :db.install/_attribute :db.part/db)))

(defn make-enum
  "Assocs :db/id to the enum."
  [enum]
  (assoc-tempid {:db/ident enum} :db.part/user))

(defn make-func
  "Assocs :db/id to the func map."
  [func]
  (assoc-tempid func :db.part/user))

(defn prepare-schema [schema]
  (-> (mapv make-part (:partitions schema))
      (into (map make-attr (:attributes schema)))
      (into (map make-enum (:enums subject)))
      (into (:functions schema))))

(defn load-base-schema
  "Loads the base schema in one transaction and derefs the result."
  [conn]
  (->> (into (build-tx {:study study
                        :study-event study-event
                        :form-ref form-ref
                        :form form
                        :item-group-ref item-group-ref
                        :item-group item-group
                        :item-ref item-ref
                        :item item
                        :alias alias
                        :code-list code-list})
             (prepare-schema base-schema))
       (d/transact conn)
       (deref)))
