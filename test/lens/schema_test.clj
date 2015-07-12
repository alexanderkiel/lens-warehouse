(ns lens.schema-test
  (:require [clojure.test :refer :all]
            [lens.schema :as schema]
            [clojure.data :refer [diff]]
            [lens.util :as util]
            [datomic.api :as d])
  (:import [clojure.lang ExceptionInfo]))

(defn- connect [] (d/connect "datomic:mem:test"))

(defn database-fixture [f]
  (do
    (d/create-database "datomic:mem:test")
    (schema/load-base-schema (connect)))
  (f)
  (d/delete-database "datomic:mem:test"))

(use-fixtures :each database-fixture)

(defmethod assert-expr 'thrown-with-data? [msg form]
  ;; (is (thrown-with-data? data expr))
  ;; Asserts that evaluating expr throws an exception with data that at least
  ;; contains the stuff in data.
  (let [data (second form)
        body (nthnext form 2)]
    `(try ~@body
          (do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch ExceptionInfo e#
            (let [d# (ex-data e#)]
              (if (first (diff ~data d#))
                (do-report {:type :fail, :message ~msg,
                            :expected '~data, :actual d#})
                (do-report {:type :pass, :message ~msg,
                            :expected '~form, :actual e#})))
            e#))))

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

(defn uuid []
  (str (d/squuid)))

(defn- create-study []
  (create :part/meta-data (fn [tid] [[:study.fn/create tid (uuid)
                                      "name-100932" "desc-145133" {}]])))

(defn- create-study-event-def [study]
  (create :part/meta-data (fn [tid] [[:study-event-def.fn/create tid
                                      (:db/id study) (uuid) "name-100952"
                                      {}]])))

(defn- create-form-def [study]
  (create :part/meta-data (fn [tid] [[:form-def.fn/create tid (:db/id study)
                                      (uuid) "name-130451" {}]])))

(defn- create-form-ref [study-event-def form-def]
  (create :part/meta-data
          (fn [tid] [[:form-ref.fn/create tid (:db/id study-event-def)
                      (:db/id form-def)]])))

(defn- update-form-def [form-def old-props new-props]
  (transact [[:form-def.fn/update (:db/id form-def) old-props new-props]]))

;; ---- Study Event -----------------------------------------------------------

;; ---- Form Ref --------------------------------------------------------------

(deftest create-form-ref-test
  (testing "Form def doesn't exist"
    (let [study-event-def (-> (create-study) (create-study-event-def))]
      (is (thrown-with-data? {:type :form-def-not-found}
                             (create-form-ref study-event-def 1)))))

  (testing "Form ref to form def exists already"
    (let [study (create-study)
          study-event-def (create-study-event-def study)
          form-def (create-form-def study)
          _ (create-form-ref study-event-def form-def)]
      (is (thrown-with-data? {:type :duplicate}
                             (create-form-ref study-event-def form-def)))))

  (testing "Succeeds"
    (let [study (create-study)
          study-event-def (create-study-event-def study)
          form-def (create-form-def study)
          form-ref (create-form-ref study-event-def form-def)
          study-event-def (:study-event-def/_form-refs form-ref)
          form-refs (:study-event-def/form-refs study-event-def)]
      (is (= 1 (count form-refs)))
      (is (= (:db/id form-def) (:db/id (:form-ref/form form-ref)))))))

;; ---- Form Def --------------------------------------------------------------

(deftest update-form-def-test
  (let [form-def (-> (create-study) (create-form-def))]
    (update-form-def form-def {} {})))

(deftest data-type-enums
  (is (entity :data-type/text))
  (is (entity :data-type/integer))
  (is (entity :data-type/float))
  (is (entity :data-type/date))
  (is (entity :data-type/time))
  (is (entity :data-type/datetime))
  (is (entity :data-type/string))
  (is (entity :data-type/boolean)))
