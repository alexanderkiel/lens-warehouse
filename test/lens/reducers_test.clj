(ns lens.reducers-test
  (:require [clojure.test :refer :all]
            [lens.reducers :refer :all])
  (:refer-clojure :exclude [empty? partition-by count]))

(deftest count-test
  (testing "zero"
    (is (= 0 (count []))))
  (testing "one"
    (is (= 1 (count [:a])))))
