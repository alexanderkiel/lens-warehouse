(ns lens.schema-test
  (:require [clojure.test :refer :all]
            [lens.schema :as schema]
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

(defn- pull [pattern eid]
  (d/pull (d/db (connect)) pattern eid))

(defn study-event [study id]
  (some->> (:study/study-events study)
           (some #(when (= id (:study-event/id %)) %))))

;; ---- Study Event -----------------------------------------------------------

(defn- create-study [id]
  (transact [[:study.fn/create #db/id[:part/meta-data] id "name-100932" {}]]))

(defn- create-study-event [study-id study-event-id]
  (transact [[:study-event.fn/create #db/id[:part/meta-data] study-id
              study-event-id "name-100952" {}]]))

(defn- create-form [study-id form-id]
  (transact [[:form.fn/create #db/id[:part/meta-data] study-id form-id
              "name-101254" {}]]))

(defn- add-form [study-id study-event-id form-id]
  (transact [[:study-event.fn/add-form study-id study-event-id form-id]]))

(deftest add-form-test
  (create-study "study-100925")
  (create-study-event "study-100925" "study-event-100948")
  (create-form "study-100925" "form-101251")
  (testing "Adding a form"
    (let [res (add-form "study-100925" "study-event-100948" "form-101251")
          study (d/entity (:db-after res) [:study/id "study-100925"])
          study-event (study-event study "study-event-100948")
          form-refs (:study-event/form-refs study-event)]
      (is (= "study-event-100948" (:study-event/id study-event)))
      (is (= "form-101251" (:form/id (:form-ref/form (first form-refs))))))))
