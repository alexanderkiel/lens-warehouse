(ns lens.handler.study-test
  (:require [clojure.test :refer :all]
            [lens.handler.study :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [clojure.edn :as edn]
            [lens.api :as api :refer [find-study-child]]))

(use-fixtures :each database-fixture)

(defn- etag [id]
  (-> (execute study-handler :get :params {:study-id id})
      (get-in [:headers "ETag"])))

(deftest study-queries
  (create-study "id-130414")

  (let [resp (execute study-handler :get
               :params {:study-id "id-130414"})]
    (are [id] (contains? (-> resp :body :queries) id)
      :lens/find-study-event-def
      :lens/find-form-def
      :lens/find-item-group-def
      :lens/find-item-def)))

(deftest study-forms
  (create-study "id-130414")

  (let [resp (execute study-handler :get
               :params {:study-id "id-130414"})]
    (are [id] (contains? (-> resp :body :forms) id)
      :lens/create-study-event-def
      :lens/create-form-def
      :lens/create-item-group-def
      :lens/create-item-def
      :lens/create-subject)))

(deftest study-handler-test
  (create-study "id-224127")

  (let [resp (execute study-handler :get
               :params {:study-id "id-224127"})]

    (is (= 200 (:status resp)))

    (testing "Body contains a self link"
      (is (= :study-handler (:handler (href resp))))
      (is (= [:study-id "id-224127"] (:args (href resp)))))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-224127" (-> resp :body :data :id)))))

  (testing "Non-conditional update fails"
    (let [resp (execute study-handler :put
                 :params {:study-id "id-224127"})]
      (is (= 400 (:status resp)))
      (is (= "Require conditional update." (error-msg resp)))))

  (testing "Update fails on missing request body"
    (create-study "id-174709" "name-202034")
    (let [resp (execute study-handler :put
                 :params {:study-id "id-201514"}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 400 (:status resp)))
      (is (= "Missing request body." (error-msg resp)))))

  (testing "Update fails on missing name and description"
    (create-study "id-174709" "name-202034")
    (let [resp (execute study-handler :put
                 :params {:study-id "id-201514"}
                 :body {:data {}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 422 (:status resp)))))

  (testing "Update fails on missing description"
    (create-study "id-174709" "name-202034")
    (let [resp (execute study-handler :put
                 :params {:study-id "id-201514"}
                 :body {:data
                        {:id "id-201514"
                         :name "name-143536"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:desc missing-required-key}"
             (error-msg resp)))))

  (testing "Update fails on ETag missmatch"
    (create-study "id-201514" "name-201516")
    (let [resp (execute study-handler :put
                 :params {:study-id "id-201514"}
                 :body {:data
                        {:id "id-201514"
                         :name "name-202906"
                         :desc "desc-105520"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 412 (:status resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (create-study "id-202032" "name-202034")
    (let [req (request :put
                :params {:study-id "id-202032"}
                :body {:data
                       {:id "id-202032"
                        :name "name-202906"
                        :desc "desc-105520"}}
                [:headers "if-match"] (etag "id-202032")
                :conn (connect))
          update (api/update-study (connect) "id-202032"
                                   {:study/name "name-202034"}
                                   {:study/name "name-203308"})
          resp (study-handler req)]
      (is (nil? update))
      (is (= 409 (:status resp)))))

  (testing "Update succeeds"
    (create-study "id-143317" "name-143321")
    (let [resp (execute study-handler :put
                 :params {:study-id "id-143317"}
                 :body {:data
                        {:id "id-143317"
                         :name "name-143536"
                         :desc "desc-105520"}}
                 [:headers "if-match"] (etag "id-143317")
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:study/name (find-study "id-143317")))))))

(deftest create-study-handler-test
  (testing "Create without id, name and description fails"
    (let [resp (execute create-handler :post
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create without name and description fails"
    (let [resp (execute create-handler :post
                 :params {:id "id-224305"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create without description fails"
    (let [resp (execute create-handler :post
                 :params {:id "id-224305" :name "name-105943"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create with blank id fails"
    (let [resp (execute create-handler :post
                 :params {:id "" :name "name-105943" :desc "desc-183610"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create with blank name fails"
    (let [resp (execute create-handler :post
                 :params {:id "id-184118" :name "" :desc "desc-183610"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create succeeds"
    (let [resp (execute create-handler :post
                 :params {:id "id-224211" :name "name-224240"
                          :desc "desc-110014"}
                 :conn (connect))]
      (is (= 201 (:status resp)))
      (is (= "id-224211" (second (:args (location resp)))))
      (is (nil? (:body resp)))))

  (testing "Create with existing id fails"
    (create-study "id-224419")
    (let [resp (execute create-handler :post
                 :params {:id "id-224419" :name "name-224240"
                          :desc "desc-110014"}
                 :conn (connect))]
      (is (= 409 (:status resp))))))
