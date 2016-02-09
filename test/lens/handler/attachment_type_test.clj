(ns lens.handler.attachment-type-test
  (:require [clojure.test :refer :all]
            [lens.handler.attachment-type :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api]
            [schema.test :refer [validate-schemas]]
            [juxt.iota :refer [given]]
            [lens.handler.util :as hu]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(defn create-attachment-type [id]
  (api/create-attachment-type (connect) id))

(deftest handler-test
  (let [eid (hu/entity-id (create-attachment-type "id-195544"))
        resp (execute handler :get
               :params {:eid eid})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (given (self-href resp)
        :handler := :attachment-type-handler
        :args := [:eid eid]))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-195544" (-> resp :body :data :id))))))

(deftest create-attachment-type-handler-test
  (testing "Create without id fails"
    (given (execute create-handler :post
             :conn (connect))
      :status := 422
      error-msg :# "Unprocessable Entity.+"
      error-msg :# ".+id.+"))

  (testing "Create with blank id fails"
    (given (execute create-handler :post
             :params {:id ""}
             :conn (connect))
      :status := 422
      error-msg :# "Unprocessable Entity.+"
      error-msg :# ".+id.+"))

  (testing "Create succeeds"
    (given (execute create-handler :post
             :params {:id "id-195804"}
             :conn (connect))
      :status := 201
      :body := nil
      [location :handler] := :attachment-type-handler
      [location :args] :> [:eid]))

  (testing "Create with existing id fails"
    (create-attachment-type "id-195820")
    (given (execute create-handler :post
             :params {:id "id-195820"}
             :conn (connect))
      :status := 409)))

(deftest all-attachment-types-handler-test
  (let [_ (create-attachment-type "id-193958")
        resp (execute all-handler :get
               :params {:page-num 1})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (given (self-href resp)
        :handler := :all-attachment-types-handler))

    (testing "Body contains one embedded attachment type"
      (is (= 1 (count (-> resp :body :embedded :lens/attachment-types)))))))
