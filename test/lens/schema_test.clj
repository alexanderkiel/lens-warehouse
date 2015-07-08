(ns lens.schema-test
  (:require [clojure.test :refer :all]
            [lens.schema :as schema]
            [lens.util :as util]
            [datomic.api :as d]))

(defn- connect [] (d/connect "datomic:mem:test"))

(defn database-fixture [f]
  (do
    (d/create-database "datomic:mem:test")
    (schema/load-base-schema (connect)))
  (f)
  (d/delete-database "datomic:mem:test"))

(use-fixtures :each database-fixture)

(defn- transact [tx-data]
  @(d/transact (connect) tx-data))

(defn- create [partition fn]
  (util/create (connect) partition fn))

(defn- entity [eid]
  (d/entity (d/db (connect)) eid))

(defn- pull [pattern eid]
  (d/pull (d/db (connect)) pattern eid))

(defn find-study-event-def [study id]
  (some->> (:study/study-event-defs study)
           (some #(when (= id (:study-event-def/id %)) %))))

;; ---- Study Event -----------------------------------------------------------

(defn- create-study [id]
  (create :part/meta-data (fn [tid] [[:study.fn/create tid id "name-100932"
                                      "desc-145133" {}]])))

(defn- create-study-event-def [study-eid study-event-def-id]
  (create :part/meta-data (fn [tid] [[:study-event-def.fn/create tid study-eid
                                      study-event-def-id "name-100952" {}]])))

(defn- create-form-def [study-eid form-def-id]
  (create :part/meta-data (fn [tid] [[:form-def.fn/create tid study-eid
                                      form-def-id "name-130451" {}]])))

(defn- add-form-def [study-event-def-eid form-def-eid]
  (transact [[:study-event-def.fn/add-form-def study-event-def-eid
              form-def-eid]]))

(defn- update-form-def [form-def-eid old-props new-props]
  (transact [[:form-def.fn/update form-def-eid old-props new-props]]))

(deftest add-form-test
  (let [study (create-study "study-100925")
        study-event-def (create-study-event-def (:db/id study)
                                                "study-event-def-100948")
        form-def (create-form-def (:db/id study) "form-def-101251")
        res (add-form-def (:db/id study-event-def) (:db/id form-def))
        study (d/entity (:db-after res) [:study/id "study-100925"])
        study-event-def (find-study-event-def study "study-event-def-100948")
        form-refs (:study-event-def/form-refs study-event-def)]
    (is (= "study-event-def-100948" (:study-event-def/id study-event-def)))
    (is (= "form-def-101251" (:form-def/id (:form-ref/form (first form-refs)))))))

(deftest update-form-test
  (let [study (create-study "study-124731")
        form-def (create-form-def (:db/id study) "form-124747")]
    (update-form-def (:db/id form-def) {} {})))

(deftest data-type-enums
  (is (entity :data-type/text))
  (is (entity :data-type/integer))
  (is (entity :data-type/float))
  (is (entity :data-type/date))
  (is (entity :data-type/time))
  (is (entity :data-type/datetime))
  (is (entity :data-type/string))
  (is (entity :data-type/boolean)))
