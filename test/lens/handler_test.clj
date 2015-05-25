(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [datomic.api :as d]
            [lens.schema :refer [load-base-schema]]
            [clojure.edn :as edn]))

(defn- connect [] (d/connect "datomic:mem:test"))

(defn database-fixture [f]
  (do
    (d/create-database "datomic:mem:test")
    (load-base-schema (connect)))
  (f)
  (d/delete-database "datomic:mem:test"))

(use-fixtures :each database-fixture)

(deftest get-subject-handler-test
  (testing "Body contains self link"
    @(d/transact (connect) [[:add-subject "id-181341"]])
    (let [path-for (fn [handler & args] {:handler handler :args args})
          req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-181341"}
               :db (d/db (connect))}
          resp ((get-subject-handler path-for) req)]
      (is (= 200 (:status resp)))
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :get-subject-handler (:handler (:href self-link))))
        (is (= [:id "id-181341"] (:args (:href self-link))))))))

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
