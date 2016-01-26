(ns lens.middleware.log-test
  (:require [clojure.test :refer :all]))

(deftest filter-response-test
  (are [resp result] (= result (#'lens.middleware.log/filter-response resp))
    {:status 200 :body "Ok"} {:status 200}
    {:status 400 :body "Error"} {:status 400 :body "Error"}))
