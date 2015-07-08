(ns lens.handler.form-def-test
  (:require [clojure.test :refer :all]
            [lens.handler.form-def :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [lens.api :as api :refer [find-study-child]]
            [datomic.api :as d]))

(use-fixtures :each database-fixture)

(defn- etag [study-id form-def-id]
  (-> (execute handler :get
        :params {:study-id study-id :form-def-id form-def-id})
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

(deftest find-form-def-handler-test
  (-> (create-study "s-183549")
      (create-form-def "id-224127"))

  (let [req (request :get
              :params {:study-id "s-183549" :id "id-224127"})
        resp (find-handler req)]

    (is (= 301 (:status resp)))

    (testing "Response contains a Location"
      (let [location (location resp)]
        (is (= :form-def-handler (:handler location)))
        (is (= [:study-id "s-183549" :form-def-id "id-224127"]
               (:args location)))))))

(deftest form-def-handler-test
  (-> (create-study "s-183549")
      (create-form-def "id-224127"))

  (let [resp (execute handler :get
               :params {:study-id "s-183549" :form-def-id "id-224127"})]

    (is (= 200 (:status resp)))

    (testing "Body contains self link"
      (is (= :form-def-handler (:handler (href resp))))
      (is (= [:study-id "s-183549" :form-def-id "id-224127"] (:args (href resp)))))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"])))

    (testing "Data contains :id"
      (is (= "id-224127" (-> resp :body :data :id)))))

  (testing "Non-conditional update fails"
    (let [resp (execute handler :put
                 :params {:study-id "s-183549" :form-def-id "id-224127"})]
      (is (= 400 (:status resp)))
      (is (= "Require conditional update." (error-msg resp)))))

  (testing "Update fails on missing name"
    (let [resp (execute handler :put
                 :params {:study-id "s-183549" :form-def-id "id-224127"}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 400 (:status resp)))
      (is (= "Missing request body." (error-msg resp)))))

  (testing "Update fails on missing name"
    (let [resp (execute handler :put
                 :params {:study-id "s-183549" :form-def-id "id-224127"}
                 :body {:data {:id "id-224127"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 422 (:status resp)))
      (is (= "Unprocessable Entity: {:name missing-required-key}"
             (error-msg resp)))))

  (testing "Update fails on ETag missmatch"
    (let [resp (execute handler :put
                 :params {:study-id "s-183549" :form-def-id "id-224127"}
                 :body {:data {:id "id-224127" :name "foo"}}
                 [:headers "if-match"] "\"foo\"")]
      (is (= 412 (:status resp)))
      (is (= "Precondition Failed" (error-msg resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (let [form-def (-> (create-study "s-095742")
                       (create-form-def "id-224127" "name-095717"))
          req (request :put
                :params {:study-id "s-095742" :form-def-id "id-224127"}
                :body {:data {:id "id-224127" :name "name-202906"}}
                [:headers "if-match"] (etag "s-095742" "id-224127")
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
          resp (execute handler :put
                 :params {:study-id "s-100836" :form-def-id "id-224127"}
                 :body {:data {:id "id-224127" :name "name-143536"}}
                 [:headers "if-match"] (etag "s-100836" "id-224127")
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:form-def/name (refresh-form-def form-def))))))

  (testing "Update can add desc"
    (let [form-def (-> (create-study "s-120219")
                       (create-form-def "id-224127" "name-095717"))
          resp (execute handler :put
                 :params {:study-id "s-120219" :form-def-id "id-224127"}
                 :body {:data {:id "id-224127"
                               :name "name-095717"
                               :desc "desc-120156"}}
                 [:headers "if-match"] (etag "s-120219" "id-224127")
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (= "desc-120156" (:form-def/desc (refresh-form-def form-def))))))

  (testing "Update can remove desc"
    (let [form-def (-> (create-study "s-100937")
                       (create-form-def "id-224127" "name-095717" "desc-120029"))
          resp (execute handler :put
                 :params {:study-id "s-100937" :form-def-id "id-224127"}
                 :body {:data {:id "id-224127" :name "name-095717"}}
                 [:headers "if-match"] (etag "s-100937" "id-224127")
                 :conn (connect))]
      (is (= 204 (:status resp)))
      (is (nil? (:form-def/desc (refresh-form-def form-def)))))))

(deftest create-form-def-handler-test
  (create-study "s-100937")

  (testing "Create without id and name fails"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-100937"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create without name fails"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-100937" :id "id-224305"}
                 :conn (connect))]
      (is (= 422 (:status resp)))))

  (testing "Create with id and name only"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-100937"
                          :id "id-224211"
                          :name "name-224240"}
                 :conn (connect))]
      (is (= 201 (:status resp)))
      (is (= "s-100937" (nth (:args (location resp)) 1)))
      (is (= "id-224211" (nth (:args (location resp)) 3)))
      (is (nil? (:body resp)))))

  (testing "Create with description"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-100937"
                          :id "id-121736"
                          :name "name-224240"
                          :desc "desc-224339"}
                 :conn (connect))]
      (is (= 201 (:status resp)))
      (is (= "desc-224339" (:form-def/desc (find-form-def "s-100937" "id-121736"))))))

  (testing "Create with existing id fails"
    (let [resp (execute create-handler :post
                 :params {:study-id "s-100937"
                          :id "id-224211"
                          :name "name-224240"}
                 :conn (connect))]
      (is (= 409 (:status resp))))))
