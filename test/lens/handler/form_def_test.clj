(ns lens.handler.form-def-test
  (:require [clojure.test :refer :all]
            [lens.handler.form-def :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api :refer [find-study-child]]
            [datomic.api :as d]
            [lens.handler.util :as hu]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each database-fixture)
(use-fixtures :once validate-schemas)

(defn- etag [eid]
  (-> (execute handler :get :params {:eid eid})
      (get-in [:headers "ETag"])))

(defn find-form-def [study-id form-def-id]
  (-> (find-study study-id)
      (find-study-child :form-def form-def-id)))

(defn- create-form-def
  ([study id] (create-form-def study id "name-182856"))
  ([study id name] (api/create-form-def (connect) study id name))
  ([study id name desc]
   (api/create-form-def (connect) study id name {:desc desc})))

(defn- refresh-form-def [form-def]
  (d/entity (d/db (connect)) (:db/id form-def)))

(deftest handler-test
  (let [eid (-> (create-study "s-183549")
                (create-form-def "id-224127")
                (hu/entity-id))
        resp (execute handler :get
               :params {:eid eid})]

    (is (= 200 (:status resp)))

    (testing "Body contains self link"
      (is (= :form-def-handler (:handler (self-href resp))))
      (is (= [:eid eid] (:args (self-href resp)))))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-224127" (-> resp :body :data :id)))))

  (testing "Non-conditional update fails"
    (let [resp (execute handler :put)]
      (is (= 428 (:status resp)))
      (is (= "Require conditional update." (error-msg resp)))))

  (testing "Update fails on missing name"
    (let [resp (execute handler :put
                 [:headers "if-match"] "\"foo\"")]
      (is (= 400 (:status resp)))
      (is (= "Missing request body." (error-msg resp)))))

  (testing "Update fails on missing name"
    (let [eid (-> (create-study "s-114628")
                  (create-form-def "id-224127")
                  (hu/entity-id))
          resp (execute handler :put
                 :params {:eid eid}
                 :body {:data {:id "id-224127"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:name missing-required-key}"
             (error-msg resp)))))

  (testing "Update fails on ETag missmatch"
    (let [eid (-> (create-study "s-114703")
                  (create-form-def "id-224127")
                  (hu/entity-id))
          resp (execute handler :put
                 :params {:eid eid}
                 :body {:data {:id "id-224127" :name "foo"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 412 (:status resp)))
      (is (= "Precondition Failed" (error-msg resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (let [form-def (-> (create-study "s-095742")
                       (create-form-def "id-224127" "name-095717"))
          eid (hu/entity-id form-def)
          req (request :put
                :params {:eid eid}
                :body {:data {:id "id-224127" :name "name-202906"}}
                [:headers "if-match"] (etag eid)
                :conn (connect))
          update (api/update-form-def (connect) form-def
                                      {:form-def/name "name-095717"}
                                      {:form-def/name "name-203308"})
          resp (handler req)]
      (is (nil? update))
      (is (= 409 (:status resp)))))

  (testing "Update name succeeds"
    (let [form-def (-> (create-study "s-100836")
                       (create-form-def "id-224127" "name-095717"))
          eid (hu/entity-id form-def)
          resp (execute handler :put
                 :params {:eid eid}
                 :body {:data {:id "id-224127" :name "name-143536"}}
                 [:headers "if-match"] (etag eid)
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:form-def/name (refresh-form-def form-def))))))

  (testing "Update can add desc"
    (let [form-def (-> (create-study "s-120219")
                       (create-form-def "id-224127" "name-095717"))
          eid (hu/entity-id form-def)
          resp (execute handler :put
                 :params {:eid eid}
                 :body {:data {:id "id-224127"
                               :name "name-095717"
                               :desc "desc-120156"}}
                 [:headers "if-match"] (etag eid)
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (= "desc-120156" (:form-def/desc (refresh-form-def form-def))))))

  (testing "Update can remove desc"
    (let [form-def (-> (create-study "s-100937")
                       (create-form-def "id-224127" "name-095717" "desc-120029"))
          eid (hu/entity-id form-def)
          resp (execute handler :put
                 :params {:eid eid}
                 :body {:data {:id "id-224127" :name "name-095717"}}
                 [:headers "if-match"] (etag eid)
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (nil? (:form-def/desc (refresh-form-def form-def)))))))

(deftest create-handler-test
  (testing "Create without id and name fails"
    (let [resp (execute create-handler :post
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create without name fails"
    (let [resp (execute create-handler :post
                 :params {:id "id-224305"}
                 :conn (connect))]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:name missing-required-key}" (error-msg resp)))))

  (testing "Create with blank id fails"
    (let [resp (execute create-handler :post
                 :params {:id "" :name "name-185223"}
                 :conn (connect))]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:id (not (non-blank? \"\"))}" (error-msg resp)))))

  (testing "Create with blank name fails"
    (let [resp (execute create-handler :post
                 :params {:id "id-224305" :name ""}
                 :conn (connect))]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:name (not (non-blank? \"\"))}" (error-msg resp)))))

  (testing "Create with id and name only"
    (let [study (create-study "s-100937")
          resp (execute create-handler :post
                 :params {:eid (hu/entity-id study)
                          :id "id-224211"
                          :name "name-224240"}
                 :conn (connect))]
      (is (= 201 (:status resp)))
      (is (= :eid (first (:args (location resp)))))
      (is (string? (second (:args (location resp)))))
      (is (nil? (:body resp)))))

  (testing "Create with description"
    (let [study (create-study "s-152055")
          resp (execute create-handler :post
                 :params {:eid (hu/entity-id study)
                          :id "id-121736"
                          :name "name-224240"
                          :desc "desc-224339"}
                 :conn (connect))]
      (is (= 201 (:status resp)))
      (is (= "desc-224339" (:form-def/desc (find-form-def "s-152055" "id-121736"))))))

  (testing "Create with existing id fails"
    (let [study (create-study "s-153736")
          _ (create-form-def study "id-153739" "name-095717" "desc-120029")
          resp (execute create-handler :post
                 :params {:eid (hu/entity-id study)
                          :id "id-153739"
                          :name "name-224240"}
                 :conn (connect))]
      (is (= 409 (:status resp)))
      (is (= "The form def exists already." (error-msg resp))))))
