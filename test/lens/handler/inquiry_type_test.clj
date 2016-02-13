(ns lens.handler.inquiry-type-test
  (:require [clojure.test :refer :all]
            [lens.handler.inquiry-type :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [schema.test :refer [validate-schemas]]
            [juxt.iota :refer [given]]
            [lens.handler.util :as hu]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(defn- etag [eid]
  (-> (execute handler :get :params {:eid eid})
      (get-in [:headers "ETag"])))

(deftest handler-test
  (let [eid (hu/entity-id (create-inquiry-type "id-195544"))
        resp (execute handler :get
               :params {:eid eid})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (given (self-href resp)
        :handler := :inquiry-type-handler
        :args := [:eid eid]))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-195544" (-> resp :body :data :id))))

    (testing "Non-conditional update fails"
      (given (execute handler :put)
        :status := 428
        error-msg := "Require conditional update."))

    (testing "Update fails on missing request body"
      (given (execute handler :put
               [:headers "if-match"] "\"foo\"")
        :status := 400
        error-msg := "Missing request body."))

    (testing "Update fails on missing id, name and rank"
      (let [eid (hu/entity-id (create-inquiry-type "id-224407"))]
        (given (execute handler :put
                 :params {:eid eid}
                 :body {:data {}}
                 [:headers "if-match"] "\"foo\"")
          :status := 422
          error-msg :# "Unprocessable Entity.+"
          error-msg :# ".+id.+"
          error-msg :# ".+name.+"
          error-msg :# ".+rank.+")))

    (testing "Update fails on ETag missmatch"
      (let [eid (hu/entity-id (create-inquiry-type "id-224536"))]
        (given (execute handler :put
                 :params {:eid eid}
                 :body {:data
                        {:id "id-224536"
                         :name "name-224547"
                         :rank 1}}
                 [:headers "if-match"] "\"foo\"")
          :status := 412)))

    (testing "Update succeeds"
      (let [eid (hu/entity-id (create-inquiry-type "id-224656"))]
        (given (execute handler :put
                 :params {:eid eid}
                 :body {:data
                        {:id "id-224656"
                         :name "name-224706"
                         :rank 1}}
                 [:headers "if-match"] (etag eid)
                 :conn (connect))
          :status := 204)
        (is (= "name-224706"
               (:inquiry-type/name (find-entity :inquiry-type eid))))))))

(deftest create-inquiry-type-handler-test
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
             :params {:id "id-195804" :name "name-224102" :rank 1}
             :conn (connect))
      :status := 201
      :body := nil
      [location :handler] := :inquiry-type-handler
      [location :args] :> [:eid]))

  (testing "Create with existing id fails"
    (create-inquiry-type "id-195820")
    (given (execute create-handler :post
             :params {:id "id-195820" :name "name-224005" :rank 1}
             :conn (connect))
      :status := 409)))

(deftest all-inquiry-types-handler-test
  (let [_ (create-inquiry-type "id-193958")
        resp (execute all-handler :get
               :params {:page-num 1})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (given (self-href resp)
        :handler := :all-inquiry-types-handler))

    (testing "Body contains one embedded inquiry type"
      (is (= 1 (count (-> resp :body :embedded :lens/inquiry-types)))))))
