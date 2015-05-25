(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]))

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
