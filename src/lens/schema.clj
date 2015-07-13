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

(defmacro func [name doc params & code]
  `{:db/id (d/tempid :db.part/user)
    :db/ident (keyword '~name)
    :db/doc ~doc
    :db/fn (d/function '{:lang "clojure" :params ~params :code (do ~@code)})})

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
         :db/ident (keyword entity-name (name attr))
         :db/valueType (keyword "db.type" (name type))
         :db/cardinality :db.cardinality/one
         :db.install/_attribute :db.part/db}
        (assoc-opts opts)
        (assoc-when :db/doc doc))))

(defn build-function [entity-name def-item]
  (update-in def-item [:db/ident] #(keyword (str entity-name ".fn") (name %))))

(defn- def-item-tx-builder [entity-name]
  (fn [def-item]
    (cond
      (sequential? def-item)
      (build-attr-map entity-name def-item)

      (:db/fn def-item)
      (build-function entity-name def-item)

      :else def-item)))

(defn- build-entity-tx [tx name def]
  (into tx (r/map (def-item-tx-builder (clojure.core/name name)) def)))

(defn- build-tx [entities]
  (reduce-kv build-entity-tx [] entities))

(def study
  "A clinical or epidemiological study. "
  [[:id :string :unique "The id of a study. Same as the study OID in ODM."]
   [:name :string :fulltext]
   [:desc :string :fulltext]
   [:protocol :ref :comp]
   [:study-event-defs :ref :many :comp]
   [:form-defs :ref :many :comp]
   [:item-group-defs :ref :many :comp]
   [:item-defs :ref :many :comp]
   [:code-lists :ref :many :comp]

   (func create
     "Creates a study."
     [db tid id name desc more]
     (if-not (d/entity db [:study/id id])
       [(merge
          {:db/id tid
           :study/id id
           :study/name name
           :study/desc desc}
          more)]
       (throw (ex-info "Duplicate." {:type :duplicate}))))

   (func update
     "Updates the study with the id.

     Ensures that the values in old-props are still current in the version of
     the in-transaction study."
     [db id old-props new-props]
     (if-let [study (d/entity db [:study/id id])]
       (let [cur-props (select-keys study (keys old-props))]
         (if (= cur-props old-props)
           (concat (for [[prop old-val] cur-props
                         :when (nil? (prop new-props))]
                     [:db/retract (:db/id study) prop old-val])
                   (for [[prop val] new-props]
                     [:db/add (:db/id study) prop val]))
           (throw (ex-info "Conflict!" {:type :conflict
                                        :old-props old-props
                                        :cur-props cur-props}))))
       (throw (ex-info "Study not found." {:type :not-found}))))])

(def protocol
  "The Protocol lists the kinds of study events that can occur within a specific
  version of a study. All clinical data must occur within one of these study
  events.

  A study whose metadata does not contain a protocol definition cannot have any
  clinical data. Such studies can serve as common metadata dictionaries --
  allowing sharing of metadata across studies."
  [[:study-event-refs :ref :many :comp]])

(def study-event-ref
  "A reference to a StudyEventDef as it occurs within a Study. The list of
  StudyEventRefs identifies the types of study events that are allowed to occur
  within the study."
  [[:study-event :ref]])

(def study-event-def
  "A StudyEventDef packages a set of forms."
  [[:id :string :index "The id of a study-event. Unique within a study."]
   [:name :string :fulltext]
   [:desc :string :fulltext]
   [:form-refs :ref :many :comp]
   [:aliases :ref :many :comp]

   (func create
     "Creates a study event.

     Ensures id uniquness within its study."
     [db tid study-eid id name more]
     (let [study (d/entity db study-eid)]
       (when-not (:study/id study)
         (throw (ex-info "Study not found." {:type :study-not-found})))
       (if-not (some #{id} (-> study :study/study-event-defs :study-event/id))
         [[:db/add (:db/id study) :study/study-event-defs tid]
          (merge
            {:db/id tid
             :study-event-def/id id
             :study-event-def/name name}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))

   (func update
     "Updates the study-event-def.

     Ensures that the values in old-props are still current in the version of
     the in-transaction study-event."
     [db study-event-def-eid old-props new-props]
     (let [study-event-def (d/entity db study-event-def-eid)]
       (if (:study-event-def/id study-event-def)
         (let [cur-props (select-keys study-event-def (keys old-props))]
           (if (= cur-props old-props)
             (concat (for [[prop old-val] cur-props
                           :when (nil? (prop new-props))]
                       [:db/retract (:db/id study-event-def) prop old-val])
                     (for [[prop val] new-props]
                       [:db/add (:db/id study-event-def) prop val]))
             (throw (ex-info "Conflict!" {:type :conflict
                                          :old-props old-props
                                          :cur-props cur-props}))))
         (throw (ex-info "Study-event not found." {:type :not-found})))))])

(def form-ref
  "A reference to a FormDef as it occurs within a specific StudyEventDef. The
  list of FormRefs identifies the types of forms that are allowed to occur
  within this type of study event."
  [[:form :ref]

   (func create
     "Creates a form-ref.

     Ensures form uniquness within its study-event-def."
     [db tid study-event-def-eid form-def-eid]
     (let [study-event-def (d/entity db study-event-def-eid)
           form-def (d/entity db form-def-eid)]
       (when-not (:study-event-def/id study-event-def)
         (throw (ex-info "Study event def not found."
                         {:type :study-event-def-not-found})))
       (when-not (:form-def/id form-def)
         (throw (ex-info "Form def not found." {:type :form-def-not-found})))
       (if-not (->> (:study-event-def/form-refs study-event-def)
                    (map (comp :db/id :form-ref/form))
                    (some #{(:db/id form-def)}))
         [[:db/add (:db/id study-event-def) :study-event-def/form-refs tid]
          {:db/id tid
           :form-ref/form (:db/id form-def)}]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))])

(def form-def
  "A form-def describes a type of form that can occur in a study."
  [[:id :string :index "The id of a form. Unique within a study."]
   [:name :string :fulltext]
   [:repeating :boolean]
   [:desc :string :fulltext]
   [:aliases :ref :many :comp]
   [:item-group-refs :ref :many :comp]

   (func create
     "Creates a form-def.

     Ensures id uniquness within its study."
     [db tid study-eid id name more]
     (let [study (d/entity db study-eid)]
       (when-not (:study/id study)
         (throw (ex-info "Study not found." {:type :study-not-found})))
       (if-not (some #{id} (map :form-def/id (:study/form-defs study)))
         [[:db/add (:db/id study) :study/form-defs tid]
          (merge
            {:db/id tid
             :form-def/id id
             :form-def/name name
             :form-def/repeating false}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))

   (func find
     "Returns the form with study-id and form-id or nil if not found."
     [db study-id form-id]
     (some->> (d/q '[:find ?f . :in $ ?sid ?fid
                     :where
                     [?s :study/id ?sid]
                     [?f :form/id ?fid]
                     [?s :study/form-defs ?f]]
                   db study-id form-id)
              (d/entity db)))

   (func update
     "Updates the form-def.

     Ensures that the values in old-props are still current in the version of
     the in-transaction form."
     [db form-def-eid old-props new-props]
     (let [form-def (d/entity db form-def-eid)]
       (if (:form-def/id form-def)
         (let [cur-props (select-keys form-def (keys old-props))]
           (if (= cur-props old-props)
             (concat (for [[prop old-val] cur-props
                           :when (nil? (prop new-props))]
                       [:db/retract (:db/id form-def) prop old-val])
                     (for [[prop val] new-props]
                       [:db/add (:db/id form-def) prop val]))
             (throw (ex-info "Conflict!" {:type :conflict
                                          :old-props old-props
                                          :cur-props cur-props}))))
         (throw (ex-info "Form not found." {:type :not-found})))))])

(def item-group-ref
  "A reference to a ItemGroupDef as it occurs within a specific FormDef. The
  list of ItemGroupRefs identifies the types of item groups that are allowed
  to occur within this type of form."
  [[:item-group :ref]

   (func create
     "Creates a item-group-ref.

     Ensures item-group uniquness within its form-def."
     [db tid form-def-eid item-group-def-eid]
     (let [form-def (d/entity db form-def-eid)
           item-group-def (d/entity db item-group-def-eid)]
       (when-not (:form-def/id form-def)
         (throw (ex-info "Form def not found." {:type :form-def-not-found})))
       (when-not (:item-group-def/id item-group-def)
         (throw (ex-info "Item group def not found." 
                         {:type :item-group-def-not-found})))
       (if-not (->> (:form-def/item-group-refs form-def)
                    (map (comp :db/id :item-group-ref/item-group))
                    (some #{(:db/id item-group-def)}))
         [[:db/add (:db/id form-def) :form-def/item-group-refs tid]
          {:db/id tid
           :item-group-ref/item-group (:db/id item-group-def)}]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))])

(def item-group-def
  "An item-group-def describes a type of item-group that can occur within a
  study."
  [[:id :string "The id of an item-group. Unique within a study."]
   [:name :string :fulltext]
   [:repeating :boolean]
   [:desc :string :fulltext]
   [:aliases :ref :many :comp]
   [:item-refs :ref :many :comp]

   (func create
     "Creates an item-group-def.

     Ensures id uniquness within its study."
     [db tid study-eid id name more]
     (let [study (d/entity db study-eid)]
       (when-not (:study/id study)
         (throw (ex-info "Study not found." {:type :study-not-found})))
       (if-not (some #{id} (map :item-group-def/id (:study/item-group-defs study)))
         [[:db/add (:db/id study) :study/item-group-defs tid]
          (merge
            {:db/id tid
             :item-group-def/id id
             :item-group-def/name name
             :item-group-def/repeating false}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))

   (func update
     "Updates the item-group-def.

     Ensures that the values in old-props are still current in the version of
     the in-transaction item-group."
     [db item-group-def-eid old-props new-props]
     (let [item-group-def (d/entity db item-group-def-eid)]
       (if (:item-group-def/id item-group-def)
         (let [cur-props (select-keys item-group-def (keys old-props))]
           (if (= cur-props old-props)
             (concat (for [[prop old-val] cur-props
                           :when (nil? (prop new-props))]
                       [:db/retract (:db/id item-group-def) prop old-val])
                     (for [[prop val] new-props]
                       [:db/add (:db/id item-group-def) prop val]))
             (throw (ex-info "Conflict!" {:type :conflict
                                          :old-props old-props
                                          :cur-props cur-props}))))
         (throw (ex-info "Form not found." {:type :not-found})))))])

(def item-ref
  "A reference to an ItemDef as it occurs within a specific ItemGroupDef. The
  list of ItemRefs identifies the types of items that are allowed to occur
  within this type of item group."
  [[:item :ref]

   (func create
     "Creates an item-ref.

     Ensures item uniquness within its item-group-def."
     [db tid item-group-def-eid item-def-eid]
     (let [item-group-def (d/entity db item-group-def-eid)
           item-def (d/entity db item-def-eid)]
       (when-not (:item-group-def/id item-group-def)
         (throw (ex-info "Item group def not found."
                         {:type :item-group-def-not-found})))
       (when-not (:item-def/id item-def)
         (throw (ex-info "Item def not found." {:type :item-def-not-found})))
       (if-not (->> (:item-group-def/item-refs item-group-def)
                    (map (comp :db/id :item-ref/item))
                    (some #{(:db/id item-def)}))
         [[:db/add (:db/id item-group-def) :item-group-def/item-refs tid]
          {:db/id tid
           :item-ref/item (:db/id item-def)}]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))])

(def item-def
  "An item-def describes a type of item that can occur within a study. Item
  properties include name, datatype, measurement units, range or codelist
  restrictions, and several other properties."
  [[:id :string "The id of an item. Unique within a study."]
   [:name :string :fulltext]
   [:data-type :ref]
   [:length :long]
   [:significant-digits :long]
   [:sds-var :ref]
   [:origin :string]
   [:comment :string :fulltext]
   [:desc :string :fulltext]
   [:question :string :fulltext]
   [:code-list :ref]
   [:aliases :ref :many :comp]

   (enum :data-type/text)
   (enum :data-type/integer)
   (enum :data-type/float)
   (enum :data-type/date)
   (enum :data-type/time)
   (enum :data-type/datetime)
   (enum :data-type/string)
   (enum :data-type/boolean)
   (enum :data-type/double)

   (enum :sds-var/sex)

   (func create
     "Creates an item-def.

     Ensures id uniquness within its study."
     [db tid study-eid id name data-type more]
     (let [study (d/entity db study-eid)]
       (when-not (:study/id study)
         (throw (ex-info "Study not found." {:type :study-not-found})))
       ;; TODO: possible performance problem to search linear
       (if-not (some #{id} (map :item-def/id (:study/item-defs study)))
         [[:db/add (:db/id study) :study/item-defs tid]
          (merge
            {:db/id tid
             :item-def/id id
             :item-def/name name
             :item-def/data-type data-type}
            more)]
         (throw (ex-info "Duplicate!" {:type :duplicate})))))

   (func update
     "Updates the item-def.

     Ensures that the values in old-props are still current in the version of
     the in-transaction item."
     [db item-def-eid old-props new-props]
     (let [item-def (d/entity db item-def-eid)]
       (if (:item-def/id item-def)
         (let [cur-props (select-keys item-def (keys old-props))]
           (if (= cur-props old-props)
             (concat (for [[prop old-val] cur-props
                           :when (nil? (prop new-props))]
                       [:db/retract (:db/id item-def) prop old-val])
                     (for [[prop val] new-props]
                       [:db/add (:db/id item-def) prop val]))
             (throw (ex-info "Conflict!" {:type :conflict
                                          :old-props old-props
                                          :cur-props cur-props}))))
         (throw (ex-info "Item not found." {:type :not-found})))))])

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

   (func create
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
  "A subject is a patient participating in the study."
  [[:study :ref]
   [:id :string :index "The id of the subject unique within its study."]

   (func create
     "Creates a subject."
     [db tid study-eid id]
     (when-not (:study/id (d/entity db study-eid))
       (throw (ex-info "Study not found." {:type :study-not-found})))
     (if-not (d/q '[:find ?sub . :in $ ?s ?id :where [?sub :subject/id ?id]
                    [?sub :subject/study ?s]] db study-eid id)
       [{:db/id tid
         :subject/study study-eid
         :subject/id id}]
       (throw (ex-info "Duplicate!" {:type :duplicate}))))])

(def study-event
  "A study event is a reusable package of forms usually corresponding to a study
  data-collection event."
  [[:subject :ref "Concrete study events are linked to a subject."]
   [:def :ref]
   [:repeat-key :string "A key used to distinguish between repeats of the same
                        type of study event for a single subject. (optional)"]

   (func create
     ""
     [db tid subject-eid study-event-def-eid]
     (when-not (:subject/id (d/entity db subject-eid))
       (throw (ex-info "Subject not found." {:type :subject-not-found})))
     (when-not (:study-event-def/id (d/entity db study-event-def-eid))
       (throw (ex-info "Study event def not found."
                       {:type :study-event-def-not-found})))
     (when (d/q '[:find ?se . :in $ ?s ?sed :where [?se :study-event/subject ?s]
                  [?se :study-event/def ?sed]]
                db subject-eid study-event-def-eid)
       (throw (ex-info "Duplicate!" {:type :duplicate})))
     [{:db/id tid
       :study-event/subject subject-eid
       :study-event/def study-event-def-eid}])])

(def form
  "A form is analogous to a page in a paper CRF book or electronic CRF screen. A
  form generally collects a set of logically and temporally related information.
  A series of forms is collected as part of a study event."
  [[:study-event :ref "Concrete forms are linked to a study-event."]
   [:def :ref]
   [:repeat-key :string "A key used to distinguish between repeats of the same
                         type of form within a single study event. (optional)"]

   (func create
     ""
     [db tid study-event-eid form-def-eid]
     (when-not (:study-event/subject (d/entity db study-event-eid))
       (throw (ex-info "Study event not found." {:type :study-event-not-found})))
     (when-not (:form-def/id (d/entity db form-def-eid))
       (throw (ex-info "Form def not found." {:type :form-def-not-found})))
     (when (d/q '[:find ?f . :in $ ?se ?fd :where [?f :form/study-event ?se]
                  [?f :form/def ?fd]]
                db study-event-eid form-def-eid)
       (throw (ex-info "Duplicate!" {:type :duplicate})))
     [{:db/id tid
       :form/study-event study-event-eid
       :form/def form-def-eid}])

   (func create-repeating
     ""
     [db tid study-event-eid form-def-eid repeat-key]
     (when-not (:study-event/subject (d/entity db study-event-eid))
       (throw (ex-info "Study event not found." {:type :study-event-not-found})))
     (when-not (:form-def/id (d/entity db form-def-eid))
       (throw (ex-info "Form def not found." {:type :form-def-not-found})))
     (when (d/q '[:find ?f . :in $ ?se ?fd ?rk :where [?f :form/study-event ?se]
                  [?f :form/def ?fd] [?f :form/repeat-key ?rk]]
                db study-event-eid form-def-eid repeat-key)
       (throw (ex-info "Duplicate!" {:type :duplicate})))
     [{:db/id tid
       :form/study-event study-event-eid
       :form/def form-def-eid
       :form/repeat-key repeat-key}])])

(def item-group
  "An item-group is a closely related set of items that are generally analyzed
  together. (item-groups are sometimes referred to as records and are associated
  with panels or tables.) item-groups are aggregated into forms."
  [[:form :ref "Concrete item-groups are linked to a form."]
   [:def :ref]
   [:repeat-key :string "A key used to distinguish between repeats of the same
                         type of item-group within a single form. (optional)"]])

(def item
  "An item is an individual clinical data item, such as a single systolic blood
  pressure reading. Items are collected together into item-groups."
  [[:form :ref "Concrete items are linked to a item-group."]
   [:def :ref]
   [:string-value :string]
   [:integer-value :long]])

(def base-schema
  {:partitions
   [{:db/ident :part/meta-data}
    {:db/ident :part/subject}
    {:db/ident :part/study-event}
    {:db/ident :part/form}]

   :attributes
   [{:db/ident :name
     :db/valueType :db.type/string
     :db/fulltext true
     :db/cardinality :db.cardinality/one
     :db/doc "The human-readable name of some entity."}

    {:db/ident :desc
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
                        :protocol protocol
                        :study-event-ref study-event-ref
                        :study-event-def study-event-def
                        :form-ref form-ref
                        :form-def form-def
                        :item-group-ref item-group-ref
                        :item-group-def item-group-def
                        :item-ref item-ref
                        :item-def item-def
                        :alias alias
                        :code-list code-list
                        :subject subject
                        :study-event study-event
                        :form form
                        :item-group item-group
                        :item item})
             (prepare-schema base-schema))
       (d/transact conn)
       (deref)))
