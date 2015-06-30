(ns lens.handler.form-def-test
  (:require [clojure.test :refer :all]
            [lens.handler.form-def :refer :all]
            [lens.handler.test-util :refer :all]
            [lens.test-util :refer :all]
            [clojure.edn :as edn]
            [lens.api :as api :refer [find-study-child]]
            [datomic.api :as d]))

(defn- create-form-def
  ([study id] (create-form-def study id "name-182856"))
  ([study id name] (api/create-form-def (connect) study id name))
  ([study id name desc]
   (api/create-form-def (connect) study id name {:desc desc})))

(defn- refresh-form-def [form-def]
  (-> (refresh-study (:study/_form-defs form-def))
      (find-study-child :form-def (:form-def/id form-def))))

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

  (let [req (request :get
              :params {:study-id "s-183549" :form-def-id "id-224127"})
        resp (handler req)]

    (is (= 200 (:status resp)))

    (testing "Body contains self link"
      (let [self-link (:self (:links (is->transit (:body resp))))
            self-link-href (edn/read-string (:href self-link))]
        (is (= :form-def-handler (:handler self-link-href)))
        (is (= [:study-id "s-183549" :form-def-id "id-224127"]
               (:args self-link-href)))))

    (testing "Response contains an ETag"
      (is (get-in resp [:headers "ETag"]))))

  (testing "Non-conditional update fails"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Require conditional update." (:body resp)))))

  (testing "Update with missing body fails"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Missing request body." (:body resp)))))

  (testing "Update with invalid body fails"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :body (str->is "{")
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Invalid request body." (:body resp)))))

  (testing "Update fails on ETag missmatch"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :body (transit->is {:name "name-202906"})
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 412 (:status resp)))))

  (testing "Update fails on missing name"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :body (transit->is {})
               :conn (connect)
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 422 (:status resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (let [form-def (-> (create-study "s-095742")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"e525c38e88c56516bf0e68fd0ba33fea\""}
               :params {:study-id "s-095742" :form-def-id "id-224127"}
               :body (transit->is {:name "name-202906"})
               :conn (connect)
               :db (d/db (connect))}
          _ (api/update-form-def (connect) form-def {:name "name-095717"}
                                 {:name "name-203308"})
          resp ((handler path-for) req)]
      (is (= 409 (:status resp)))))

  (testing "Update with JSON succeeds"
    (let [form-def (-> (create-study "s-100747")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"26f096525c26dec3f2229c94e58bd384\""}
               :params {:study-id "s-100747" :form-def-id "id-224127"}
               :body (transit->is {:name "name-202906"})
               :conn (connect)
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (= "name-202906" (:name (refresh-form-def form-def))))))

  (testing "Update with Transit succeeds"
    (let [form-def (-> (create-study "s-100836")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/transit+json"
                         "content-type" "application/transit+json"
                         "if-match" "\"591fab8cb5dfbc25a29cc4e441c98eb3\""}
               :params {:study-id "s-100836" :form-def-id "id-224127"}
               :body (transit->is {:name "name-143536"})
               :conn (connect)
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:name (refresh-form-def form-def))))))

  (testing "Update can remove desc"
    (let [form-def (-> (create-study "s-100937")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/transit+json"
                         "content-type" "application/transit+json"
                         "if-match" "\"2aa41ee85bc0c96a32cac4b7d5290280\""}
               :params {:study-id "s-100937" :form-def-id "id-224127"}
               :body (transit->is {:name "name-143536"})
               :conn (connect)
               :db (d/db (connect))}
          resp ((handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (nil? (:desc (refresh-form-def form-def)))))))

(deftest create-form-def-handler-test
  (create-study "s-100937")

  (testing "Create without id and name fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937"}
               :conn (connect)}]
      (is (= 422 (:status ((create-handler nil) req))))))

  (testing "Create without name fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224305"}
               :conn (connect)}]
      (is (= 422 (:status ((create-handler nil) req))))))

  (testing "Create with id and name only"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224211"
                        :name "name-224240"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-handler path-for) req)]
      (is (= 201 (:status resp)))
      (let [location (location resp)]
        (is (= [:study-id "s-100937" :form-def-id "id-224211"]
               (:args location))))
      (is (nil? (:body resp)))))

  (testing "Create with description"
    (let [req {:request-method :post
               :params {:study-id "s-100937"
                        :id "id-224401"
                        :name "name-224330"
                        :desc "desc-224339"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-handler path-for) req)]
      (is (= 201 (:status resp)))
      (let [form-def (-> (find-study "s-100937")
                         (find-study-child :form-def "id-224401"))]
        (is (= "desc-224339" (:desc form-def))))))

  (testing "Create with existing id fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224401"
                        :name "name-224439"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-handler path-for) req)]
      (is (= 409 (:status resp))))))
