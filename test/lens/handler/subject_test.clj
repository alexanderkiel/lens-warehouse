(ns lens.handler.subject-test
  (:require [clojure.test :refer :all]
            [lens.handler.subject :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api]))

(use-fixtures :each database-fixture)

(defn- create-subject [study id]
  (api/create-subject (connect) study id))

(deftest handler-test
  (-> (create-study "s-172046")
      (create-subject "sub-172208"))

  (testing "Body contains self link"
    (let [resp (execute handler :get
                 :params {:study-id "s-172046" :subject-id "sub-172208"})]
      (is (= 200 (:status resp)))

      (testing "Body contains a self link"
        (is (= :subject-handler (:handler (href resp))))
        (is (= [:study-id "s-172046" :subject-id "sub-172208"] (:args (href resp)))))

      (testing "contains the id"
        (is (= "sub-172208" (-> resp :body :data :id)))))))

(deftest create-handler-test
  (-> (create-study "s-174305")
      (create-subject "id-182721"))

  (testing "Create without study id fails"
    (let [resp (execute create-handler :post
                 :params {})]
      (is (= 422 (:status resp)))))

  (testing "Create without id fails"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-174305"})]
      (is (= 422 (:status resp)))))

  (testing "Create with existing id fails"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-174305" :id "id-182721"}
                 :conn (connect))]
      (is (= 409 (:status resp)))))

  (testing "Create with id only"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-174305" :id "id-165339"}
                 :conn (connect))]
      (is (= 201 (:status resp)))

      (let [location (location resp)]
        (is (= "s-174305" (nth (:args location) 1)))
        (is (= "id-165339" (nth (:args location) 3))))
      (is (nil? (:body resp))))))
