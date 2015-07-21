(ns lens.handler.study-test
  (:require [clojure.test :refer :all]
            [lens.handler.study :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api]
            [lens.handler.util :as hu]
            [schema.test :refer [validate-schemas]]
            [juxt.iota :refer [given]]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(defn- etag [eid]
  (-> (execute handler :get :params {:eid eid})
      (get-in [:headers "ETag"])))

(deftest study-queries
  (let [eid (hu/entity-id (create-study "id-130414"))
        resp (execute handler :get
               :params {:eid eid})]
    (are [id] (contains? (-> resp :body :queries) id)
      :lens/find-study-event-def
      :lens/find-form-def
      :lens/find-item-group-def
      :lens/find-item-def)))

(deftest study-forms
  (let [eid (hu/entity-id (create-study "id-130414"))
        resp (execute handler :get
               :params {:eid eid})]
    (are [id] (contains? (-> resp :body :forms) id)
      :lens/create-study-event-def
      :lens/create-form-def
      :lens/create-item-group-def
      :lens/create-item-def
      :lens/create-subject)))

(deftest handler-test
  (let [eid (hu/entity-id (create-study "id-224127"))
        resp (execute handler :get
               :params {:eid eid})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (given (href resp)
        :handler := :study-handler
        :args := [:eid eid]))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-224127" (-> resp :body :data :id)))))

  (testing "Non-conditional update fails"
    (given (execute handler :put)
      :status := 400
      error-msg' := "Require conditional update."))

  (testing "Update fails on missing request body"
    (given (execute handler :put
             [:headers "if-match"] "\"foo\"")
      :status := 400
      error-msg' := "Missing request body."))

  (testing "Update fails on missing id, name and description"
    (let [eid (hu/entity-id (create-study "id-174709"))]
      (given (execute handler :put
               :params {:eid eid}
               :body {:data {}}
               [:headers "if-match"] "\"foo\"")
        :status := 422
        error-msg' :# "Unprocessable Entity.+"
        error-msg' :# ".+id.+"
        error-msg' :# ".+name.+"
        error-msg' :# ".+desc.+")))

  (testing "Update fails on missing description"
    (let [eid (hu/entity-id (create-study "id-113833" "name-202034"))]
      (given (execute handler :put
               :params {:eid eid}
               :body {:data
                      {:id "id-113833"
                       :name "name-143536"}}
               [:headers "if-match"] "\"foo\"")
        :status := 422
        error-msg' :# "Unprocessable Entity.+"
        error-msg' :!# ".+id.+"
        error-msg' :!# ".+name.+"
        error-msg' :# ".+desc.+")))

  (testing "Update fails on ETag missmatch"
    (let [eid (hu/entity-id (create-study "id-201514" "name-201516"))]
      (given (execute handler :put
               :params {:eid eid}
               :body {:data
                      {:id "id-201514"
                       :name "name-202906"
                       :desc "desc-105520"}}
               [:headers "if-match"] "\"foo\"")
        :status := 412)))

  (testing "Update fails in-transaction on name missmatch"
    (let [eid (hu/entity-id (create-study "id-114012" "name-202034"))
          req (request :put
                :params {:eid eid}
                :body {:data
                       {:id "id-114012"
                        :name "name-202906"
                        :desc "desc-105520"}}
                [:headers "if-match"] (etag eid)
                :conn (connect))
          update (api/update-study (connect) "id-114012"
                                   {:study/name "name-202034"}
                                   {:study/name "name-203308"})]
      (is (nil? update))
      (given (handler req)
        :status := 409
        error-msg' := "Conflict")))

  (testing "Update succeeds"
    (let [eid (hu/entity-id (create-study "id-143317" "name-143321"))]
      (given (execute handler :put
               :params {:eid eid}
               :body {:data
                      {:id "id-143317"
                       :name "name-143536"
                       :desc "desc-105520"}}
               [:headers "if-match"] (etag eid)
               :conn (connect))
        :status := 204)
      (is (= "name-143536" (:study/name (find-study "id-143317")))))))

(deftest create-study-handler-test
  (testing "Create without id, name and description fails"
    (given (execute create-handler :post
             :conn (connect))
      :status := 422
      error-msg' :# "Unprocessable Entity.+"
      error-msg' :# ".+id.+"
      error-msg' :# ".+name.+"
      error-msg' :# ".+desc.+"))

  (testing "Create without name and description fails"
    (given (execute create-handler :post
             :params {:id "id-224305"}
             :conn (connect))
      :status := 422
      error-msg' :# "Unprocessable Entity.+"
      error-msg' :!# ".+id.+"
      error-msg' :# ".+name.+"
      error-msg' :# ".+desc.+"))

  (testing "Create without description fails"
    (given (execute create-handler :post
             :params {:id "id-224305" :name "name-105943"}
             :conn (connect))
      :status := 422
      error-msg' :# "Unprocessable Entity.+"
      error-msg' :!# ".+id.+"
      error-msg' :!# ".+name.+"
      error-msg' :# ".+desc.+"))

  (testing "Create with blank id fails"
    (given (execute create-handler :post
             :params {:id "" :name "name-105943" :desc "desc-183610"}
             :conn (connect))
      :status := 422
      error-msg' :# "Unprocessable Entity.+"
      error-msg' :# ".+id.+"
      error-msg' :!# ".+name.+"
      error-msg' :!# ".+desc.+"))

  (testing "Create with blank name fails"
    (given (execute create-handler :post
             :params {:id "id-184118" :name "" :desc "desc-183610"}
             :conn (connect))
      :status := 422
      error-msg' :# "Unprocessable Entity.+"
      error-msg' :!# ".+id.+"
      error-msg' :# ".+name.+"
      error-msg' :!# ".+desc.+"))

  (testing "Create succeeds"
    (given (execute create-handler :post
             :params {:id "id-224211" :name "name-224240"
                      :desc "desc-110014"}
             :conn (connect))
      :status := 201
      :body := nil
      [:headers "Location"] :# ".+study-handler.+"
      [:headers "Location"] :# ".+eid.+"))

  (testing "Create with existing id fails"
    (create-study "id-224419")
    (given (execute create-handler :post
             :params {:id "id-224419" :name "name-224240"
                      :desc "desc-110014"}
             :conn (connect))
      :status := 409)))

(deftest find-handler-test
  (create-study "s-154909")

  (let [resp (execute find-handler :get
               :params {:id "s-154909"})]

    (is (= 301 (:status resp)))

    (testing "Response contains a Location"
      (given (location resp)
        :handler := :study-handler
        :args :> [:eid])))

  (testing "Fails on missing id"
    (given (execute find-handler :get
             :params {})
      :status := 422
      error-msg' := "Unprocessable Entity")))

(deftest find-child-handler-test
  (let [study (create-study "s-183549")
        _ (api/create-form-def (connect) study "id-224127" "name-124505")
        resp (execute (find-child-handler :form-def) :get
               :params {:eid (hu/entity-id study) :id "id-224127"})]

    (is (= 301 (:status resp)))

    (testing "Response contains a Location"
      (given (location resp)
        :handler := :form-def-handler
        :args :> [:eid])))

  (testing "Fails on missing id"
    (given (execute (find-child-handler :form-def) :get
             :params {:eid 1})
      :status := 422
      error-msg' :# "Unprocessable Entity.+")))
