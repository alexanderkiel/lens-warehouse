(ns lens.handler.subject-test
  (:require [clojure.test :refer :all]
            [lens.handler.subject :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api]
            [schema.test :refer [validate-schemas]]
            [lens.handler.util :as hu]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(defn- create-subject [study id]
  (api/create-subject (connect) study id))

(deftest handler-test
  (testing "Body contains self link"
    (let [subject (-> (create-study "s-172046") (create-subject "id-172208"))
          resp (execute handler :get
                 :params {:eid (hu/entity-id subject)})]
      (is (= 200 (:status resp)))

      (testing "Body contains a self link"
        (is (= :subject-handler (:handler (self-href resp))))
        (is (= :eid (first (:args (self-href resp)))))
        (is (string? (second (:args (self-href resp))))))

      (testing "contains the id"
        (is (= "id-172208" (-> resp :body :data :id)))))))

(deftest create-handler-test
  (testing "Create without id fails"
    (let [resp (execute create-handler :post
                 :params {})]
      (is (= 422 (:status resp)))))

  (testing "Create with existing id fails"
    (let [study (create-study "s-174305")
          _ (create-subject study "id-182721")
          resp (execute create-handler :post
                 :params {:eid (hu/entity-id study) :id "id-182721"}
                 :conn (connect))]
      (is (= 409 (:status resp)))))

  (testing "Create with id only"
    (let [study (create-study "s-150908")
          _ (create-subject study "id-182721")
          resp (execute create-handler :post
                 :params {:eid (hu/entity-id study) :id "id-165339"}
                 :conn (connect))]
      (is (= 201 (:status resp)))

      (let [location (location resp)]
        (is (= :eid (first (:args location))))
        (is (string? (second (:args location)))))

      (is (nil? (:body resp))))))
