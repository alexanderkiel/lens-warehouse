(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [lens.test-util :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [schema.test :refer [validate-schemas]]
            [juxt.iota :refer [given]]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(deftest service-document-handler-test
  (let [resp (execute (service-document-handler "") :get)]

    (is (= 200 (:status resp)))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :name Lens Warehouse"
      (is (= "Lens Warehouse" (-> resp :body :data :name))))

    (testing "Body contains a self link"
      (given (self-href resp)
        :handler := :service-document-handler))

    (testing "Body contains a find study query"
      (given (-> resp :body :queries :lens/find-study)
        [href :handler] := :find-study-handler))))

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
