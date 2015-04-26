(ns lens.schema
  "Functions to load the schema.

  Usage:

    (load-base-schema conn)"
  (:use plumbing.core)
  (:require [slingshot.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :refer [debug]]
            [datomic.api :as d]
            [lens.util :as util]))

(def base-schema
  {:partitions
   [{:db/ident :part/subject}
    {:db/ident :part/meta-data}
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

    ;; Subject

    {:db/ident :subject/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The identifier of a subject."}

    ;; Study

    {:db/ident :study/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of a study."}

    ;; Study-Event

    {:db/ident :study-event/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of a study-event."}

    ;; Form

    {:db/ident :form/id
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
     :db/doc "A reference to all studies of a form."}

    ;; Item-Group

    {:db/ident :item-group/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of an item."}

    {:db/ident :item-group/form
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the form of an item group."}

    {:db/ident :item-group/rank
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "The rank of an item group relative to its form."}

    ;; Item

    {:db/ident :item/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of an item."}

    {:db/ident :item/question
     :db/valueType :db.type/string
     :db/fulltext true
     :db/cardinality :db.cardinality/one
     :db/doc "The human-readable question of an item."}

    {:db/ident :item/item-group
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the item group of an item."}

    {:db/ident :item/rank
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "The rank of an item relative to its item group."}

    {:db/ident :item/code-list
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the code list of an item."}

    {:db/ident :item/attr
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the data point attribute of an item."}

    ;; Code-List

    {:db/ident :code-list/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "The id of a code list."}

    {:db/ident :code-list/attr
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A reference to the code list item attribute of a code list."}

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
                  "code-list-item or study-event.")}]

   :functions
   [{:db/id (d/tempid :db.part/user)
     :db/ident :add-subject
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id]
         :code [{:db/id (d/tempid :part/subject)
                 :subject/id id}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :retract-subject
     :db/doc "Retracts a subject including all its visits."
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [db id]
         :code
         (if-let [subject (d/entity db [:subject/id id])]
           (->> (:visit/_subject subject)
                (map #(vector :db.fn/retractEntity (:db/id %)))
                (cons [:db.fn/retractEntity (:db/id subject)]))
           (throw (ex-info (str "Unknown subject: " id)
                           {:type :unknown-subject :id id})))})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-study
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id]
         :code [{:db/id (d/tempid :part/meta-data)
                 :study/id id}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-study-event
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id]
         :code [{:db/id (d/tempid :part/meta-data)
                 :study-event/id id}]})}
    {:db/id (d/tempid :db.part/user)
     :db/ident :add-form
     :db/fn
     (d/function
       '{:lang "clojure"
         :params [_ id]
         :code [{:db/id (d/tempid :part/meta-data)
                 :form/id id}]})}
    {:db/id (d/tempid :db.part/user)
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

(defn prepare-schema [schema]
  (-> (mapv util/make-part (:partitions schema))
      (into (mapv util/make-attr (:attributes schema)))
      (into (:functions schema))))

(defn load-base-schema
  "Loads the base schema in one transaction and derefs the result."
  [conn]
  (->> (prepare-schema base-schema)
       (d/transact conn)
       (deref)))
