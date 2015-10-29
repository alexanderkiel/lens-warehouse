(ns lens.route-test
  (:require [clojure.test :refer :all]
            [lens.route :refer :all]
            [schema.test :refer [validate-schemas]]
            [bidi.bidi :as bidi]))

(use-fixtures :once validate-schemas)

(deftest routes-test
  (testing "Root Context Path is ok"
    (is (routes "")))

  (testing "Context Path starting and not ending with a slash is ok"
    (is (routes "/a")))

  (testing "Context Path with two path segments is ok"
    (is (routes "/a/b")))

  (testing "Context Path ending with a slash is invalid"
    (is (thrown? Exception (routes "/")))
    (is (thrown? Exception (routes "/a/"))))

  (testing "Double slash Context Path is invalid"
    (is (thrown? Exception (routes "//"))))

  (testing "With context path /112838"
    (are [handler params path] (= path (apply bidi/path-for (routes "/112838")
                                              handler params))
      :service-document-handler [] "/112838/"
      :item-def-profile-handler [] "/112838/p/id")))
