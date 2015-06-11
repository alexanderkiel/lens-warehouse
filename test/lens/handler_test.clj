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
  (let [study (create-study "s-172046")]
    (create-subject study "sub-172208")
    (testing "Body contains self link"
      (let [req {:request-method :get
                 :headers {"accept" "application/edn"}
                 :params {:study-id "s-172046" :subject-id "sub-172208"}
                 :db (d/db (connect))}
            resp ((subject-handler path-for) req)]
        (is (= 200 (:status resp)))
        (let [self-link (:self (:links (edn/read-string (:body resp))))
              self-link-href (edn/read-string (:href self-link))]
          (is (= :subject-handler (:handler self-link-href)))
          (is (= [:study-id "s-172046" :subject-id "sub-172208"]
                 (:args self-link-href))))))))

(deftest create-subject-handler-test
  (let [study (create-study "s-174305")]
    (testing "Create without study id fails"
      (let [req {:request-method :post
                 :params {}
                 :conn (connect)
                 :db (d/db (connect))}]
        (is (= 422 (:status ((create-subject-handler nil) req))))))

    (testing "Create without id fails"
      (let [req {:request-method :post
                 :params {:study-id "s-174305"}
                 :conn (connect)
                 :db (d/db (connect))}]
        (is (= 422 (:status ((create-subject-handler nil) req))))))

    (testing "Create with id only"
      (let [req {:request-method :post
                 :params {:study-id "s-174305" :id "id-165339"}
                 :conn (connect)
                 :db (d/db (connect))}
            resp ((create-subject-handler path-for) req)]
        (is (= 201 (:status resp)))
        (let [location (location resp)]
          (is (= "s-174305" (nth (:args location) 1)))
          (is (= "id-165339" (nth (:args location) 3))))
        (is (nil? (:body resp)))))

    (testing "Create with existing id fails"
      (create-subject study "id-182721")
      (let [req {:request-method :post
                 :params {:study-id "s-174305" :id "id-182721"}
                 :conn (connect)
                 :db (d/db (connect))}
            resp ((create-subject-handler path-for) req)]
        (is (= 409 (:status resp)))))))

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
