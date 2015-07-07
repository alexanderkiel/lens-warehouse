(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [datomic.api :as d]
            [lens.schema :refer [load-base-schema]]
            [clojure.edn :as edn]
            [lens.api :as api :refer [find-study-child]]))

(use-fixtures :each database-fixture)

;; ---- Subject ---------------------------------------------------------------

(defn- create-subject [study id]
  (api/create-subject (connect) study id))

(deftest get-subject-handler-test
  (-> (create-study "s-172046")
      (create-subject "sub-172208"))

  (testing "Body contains self link"
    (let [resp (execute subject-handler :get
                 :params {:study-id "s-172046" :subject-id "sub-172208"})]
      (is (= 200 (:status resp)))

      (testing "Body contains a self link"
        (is (= :subject-handler (:handler (href resp))))
        (is (= [:study-id "s-172046" :subject-id "sub-172208"] (:args (href resp)))))

      (testing "contains the id"
        (is (= "sub-172208" (-> resp :body :data :id)))))))

(deftest create-subject-handler-test
  (-> (create-study "s-174305")
      (create-subject "id-182721"))

  (testing "Create without study id fails"
    (let [resp (execute create-subject-handler :post
                 :params {})]
      (is (= 422 (:status resp)))))

  (testing "Create without id fails"
    (let [resp (execute create-subject-handler :post
                 :params {:study-id "s-174305"})]
      (is (= 422 (:status resp)))))

  (testing "Create with existing id fails"
    (let [resp (execute create-subject-handler :post
                 :params {:study-id "s-174305" :id "id-182721"}
                 :conn (connect))]
      (is (= 409 (:status resp)))))

  (testing "Create with id only"
    (let [resp (execute create-subject-handler :post
                 :params {:study-id "s-174305" :id "id-165339"}
                 :conn (connect))]
      (is (= 201 (:status resp)))

      (let [location (location resp)]
        (is (= "s-174305" (nth (:args location) 1)))
        (is (= "id-165339" (nth (:args location) 3))))
      (is (nil? (:body resp))))))

;; ----------------------------------------------------------------------------

(defn- visit [birth-date edat]
  {:visit/subject {:subject/birth-date (tc/to-date birth-date)}
   :visit/edat (tc/to-date edat)})

(deftest age-at-visit-test
  (testing "age of birth date edat is zero"
    (is (= 0 (age-at-visit (visit (t/date-time 2015 05 11)
                                  (t/date-time 2015 05 11))))))
  (testing "1 year age"
    (is (= 1 (age-at-visit (visit (t/date-time 2014 05 11)
                                  (t/date-time 2015 05 11))))))
  (testing "2 year age"
    (is (= 2 (age-at-visit (visit (t/date-time 2014 05 11)
                                  (t/date-time 2016 05 11))))))
  (testing "-1 year age"
    (is (= -1 (age-at-visit (visit (t/date-time 2014 05 11)
                                   (t/date-time 2013 05 11)))))))
