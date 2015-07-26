(ns lens.route-test
  (:require [clojure.test :refer :all]
            [lens.route :refer :all]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

(deftest routes-test
  (testing "Blank Context Path is invalid"
    (is (thrown? Exception (routes ""))))

  (testing "Root Context Path (slash) is ok"
    (is (routes "/")))

  (testing "Context Path starting with slash is ok"
    (is (routes "/a")))

  (testing "Context Path ending with slash is invalid"
    (is (thrown? Exception (routes "/a/"))))

  (testing "Double slash Context Path is invalid"
    (is (thrown? Exception (routes "//")))))
