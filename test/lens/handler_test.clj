(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [datomic.api :as d]
            [lens.schema :refer [load-base-schema]]
            [clojure.edn :as edn]
            [lens.api :as api]))

(defn- path-for [handler & args] {:handler handler :args args})

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
    (api/create-subject (connect) "id-181341")
    (let [req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-181341"}
               :db (d/db (connect))}
          resp ((get-subject-handler path-for) req)]
      (is (= 200 (:status resp)))
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :get-subject-handler (:handler (:href self-link))))
        (is (= [:id "id-181341"] (:args (:href self-link))))))))

(deftest create-subject-handler-test
  (testing "Create without id fails"
    (let [req {:request-method :post
               :params {}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))
  (testing "Create with invalid sex fails"
    (let [req {:request-method :post
               :params {:id "id-172029"
                        :sex "foo"}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))
  (testing "Create with invalid birth-date fails"
    (let [req {:request-method :post
               :params {:id "id-172029"
                        :birth-date "foo"}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))
  (testing "Create with id only"
    (let [path-for (fn [_ _ id] id)
          req {:request-method :post
               :params {:id "id-165339"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= "id-165339" (get-in resp [:headers "Location"])))
      (is (nil? (:body resp)))))
  (testing "Create with sex"
    (let [req {:request-method :post
               :params {:id "id-171917"
                        :sex "male"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= :subject.sex/male
             (:subject/sex (api/subject (d/db (connect)) "id-171917"))))))
  (testing "Create with birth-date"
    (let [req {:request-method :post
               :params {:id "id-173133"
                        :birth-date "2015-05-25"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= (tc/to-date (t/date-time 2015 5 25))
             (:subject/birth-date (api/subject (d/db (connect)) "id-173133"))))))
  (testing "Create with existing id fails"
    (api/create-subject (connect) "id-182721")
    (let [req {:request-method :post
               :params {:id "id-182721"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 409 (:status resp))))))

(deftest get-study-handler-test
  (testing "Body contains self link"
    (api/create-study (connect) "id-224127" "name-224123")
    (let [req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-224127"}
               :db (d/db (connect))}
          resp ((get-study-handler path-for) req)]
      (is (= 200 (:status resp)))
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :get-study-handler (:handler (:href self-link))))
        (is (= [:id "id-224127"] (:args (:href self-link))))))))

(deftest create-study-handler-test
  (testing "Create without id and name fails"
    (let [req {:request-method :post
               :params {}
               :conn (connect)}]
      (is (= 422 (:status ((create-study-handler nil) req))))))
  (testing "Create without name fails"
    (let [req {:request-method :post
               :params {:id "id-224305"}
               :conn (connect)}]
      (is (= 422 (:status ((create-study-handler nil) req))))))
  (testing "Create with id and name only"
    (let [path-for (fn [_ _ id] id)
          req {:request-method :post
               :params {:id "id-224211" :name "name-224240"}
               :conn (connect)}
          resp ((create-study-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= "id-224211" (get-in resp [:headers "Location"])))
      (is (nil? (:body resp)))))
  (testing "Create with description"
    (let [req {:request-method :post
               :params {:id "id-224401"
                        :name "name-224330"
                        :description "description-224339"}
               :conn (connect)}
          resp ((create-study-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= "description-224339"
             (:description (api/study (d/db (connect)) "id-224401"))))))
  (testing "Create with existing id fails"
    (api/create-study (connect) "id-224419" "name-224431")
    (let [req {:request-method :post
               :params {:id "id-224419" :name "name-224439"}
               :conn (connect)}
          resp ((create-study-handler path-for) req)]
      (is (= 409 (:status resp))))))

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
