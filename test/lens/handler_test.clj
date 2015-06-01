(ns lens.handler-test
  (:require [clojure.test :refer :all]
            [lens.handler :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [datomic.api :as d]
            [lens.schema :refer [load-base-schema]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [cognitect.transit :as transit]
            [lens.api :as api :refer [find-form-def]])
  (:import [java.io ByteArrayOutputStream]))

(defn- str->is [s]
  (io/input-stream (.getBytes s "utf-8")))

(defn- transit->is [o]
  (let [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :json) o)
    (io/input-stream (.toByteArray out))))

(defn- path-for [handler & args] {:handler handler :args args})

(defn- connect [] (d/connect "datomic:mem:test"))

(defn database-fixture [f]
  (do
    (d/create-database "datomic:mem:test")
    (load-base-schema (connect)))
  (f)
  (d/delete-database "datomic:mem:test"))

(use-fixtures :each database-fixture)

;; ---- Study -----------------------------------------------------------------

(defn- find-study [id]
  (api/find-study (d/db (connect)) id))

(defn- create-study
  ([id] (create-study id "name-172037"))
  ([id name]
   (api/create-study (connect) id name))
  ([id name description]
   (api/create-study (connect) id name {:description description})))

(defn- refresh-study [study]
  (api/find-study (d/db (connect)) (:study/id study)))

(deftest study-handler-test
  (testing "Body contains self link"
    (create-study "id-224127")
    (let [req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-224127"}
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 200 (:status resp)))
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :study-handler (:handler (:href self-link))))
        (is (= [:id "id-224127"] (:args (:href self-link)))))))

  (testing "Response contains an ETag"
    (create-study "id-175847" "name-175850")
    (let [req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-175847"}
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= "\"23a61f1f52398fcd599d61672a63815d\""
             (get-in resp [:headers "ETag"])))))

  (testing "Non-conditional update fails"
    (create-study "id-093946" "name-201516")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"}
               :params {:id "id-093946"}
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Require conditional update." (:body resp)))))

  (testing "Update with missing body fails"
    (create-study "id-201514" "name-201516")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:id "id-201514"}
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Missing request body." (:body resp)))))

  (testing "Update with invalid body fails"
    (create-study "id-201514" "name-201516")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:id "id-201514"}
               :body (str->is "{")
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Invalid request body." (:body resp)))))

  (testing "Update fails on ETag missmatch"
    (create-study "id-201514" "name-201516")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:id "id-201514"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 412 (:status resp)))))

  (testing "Update fails on missing name"
    (create-study "id-174709" "name-202034")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:id "id-174709"}
               :body (str->is "{}")
               :conn (connect)
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 422 (:status resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (create-study "id-202032" "name-202034")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"aff8423a097f1e8ebcfcf63319d86f83\""}
               :params {:id "id-202032"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :db (d/db (connect))}
          _ (api/update-study (connect) "id-202032" {:name "name-202034"}
                              {:name "name-203308"})
          req (assoc req :conn (connect))
          resp ((study-handler path-for) req)]
      (is (= 409 (:status resp)))))

  (testing "Update with JSON succeeds"
    (create-study "id-203855" "name-202034")
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"a5f78306eefab2042ff77d3a67567e1b\""}
               :params {:id "id-203855"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :conn (connect)
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (= "name-202906" (:name (find-study "id-203855"))))))

  (testing "Update with Transit succeeds"
    (create-study "id-143317" "name-143321")
    (let [req {:request-method :put
               :headers {"accept" "application/transit+json"
                         "content-type" "application/transit+json"
                         "if-match" "\"ee42fd30011f499d4f9147fb2491deb7\""}
               :params {:id "id-143317"}
               :body (transit->is {:name "name-143536"})
               :conn (connect)
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:name (find-study "id-143317"))))))

  (testing "Update can remove description"
    (create-study "id-143317" "name-143321" "desc-155658")
    (let [req {:request-method :put
               :headers {"accept" "application/transit+json"
                         "content-type" "application/transit+json"
                         "if-match" "\"e5d89c09239bde70472e7c250170429b\""}
               :params {:id "id-143317"}
               :body (transit->is {:name "name-143536"})
               :conn (connect)
               :db (d/db (connect))}
          resp ((study-handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (nil? (:description (find-study "id-143317")))))))

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
             (:description (api/find-study (d/db (connect)) "id-224401"))))))

  (testing "Create with existing id fails"
    (api/create-study (connect) "id-224419" "name-224431")
    (let [req {:request-method :post
               :params {:id "id-224419" :name "name-224439"}
               :conn (connect)}
          resp ((create-study-handler path-for) req)]
      (is (= 409 (:status resp))))))

;; ---- Subject ---------------------------------------------------------------

(defn- create-subject [study id]
  (api/create-subject (connect) study id))

(deftest get-subject-handler-test
  (let [study (create-study "s-172046")]
    (create-subject study "sub-172208")
    (testing "Body contains self link"
      (let [req {:request-method :get
                 :headers {"accept" "application/edn"}
                 :params {:study-id "s-172046" :subject-id "sub-172208"}
                 :db (d/db (connect))}
            resp ((get-subject-handler path-for) req)]
        (is (= 200 (:status resp)))
        (let [self-link (:self (:links (edn/read-string (:body resp))))]
          (is (= :get-subject-handler (:handler (:href self-link))))
          (is (= [:study-id "s-172046" :subject-id "sub-172208"]
                 (:args (:href self-link)))))))))

(deftest create-subject-handler-test
  (let [study (create-study "s-174305")]
    (testing "Create without study id fails"
      (let [req {:request-method :post
                 :params {}
                 :conn (connect)
                 :db (d/db (connect))}]
        (is (= 422 (:status ((create-subject-handler nil) req))))))

    (testing "Create without id fails"
      (let [req {:request-method :post
                 :params {:study-id "s-174305"}
                 :conn (connect)
                 :db (d/db (connect))}]
        (is (= 422 (:status ((create-subject-handler nil) req))))))

    (testing "Create with id only"
      (let [req {:request-method :post
                 :params {:study-id "s-174305" :id "id-165339"}
                 :conn (connect)
                 :db (d/db (connect))}
            resp ((create-subject-handler path-for) req)]
        (is (= 201 (:status resp)))
        (let [location (edn/read-string (get-in resp [:headers "Location"]))]
          (is (= "s-174305" (nth (:args location) 1)))
          (is (= "id-165339" (nth (:args location) 3))))
        (is (nil? (:body resp)))))

    (testing "Create with existing id fails"
      (create-subject study "id-182721")
      (let [req {:request-method :post
                 :params {:study-id "s-174305" :id "id-182721"}
                 :conn (connect)
                 :db (d/db (connect))}
            resp ((create-subject-handler path-for) req)]
        (is (= 409 (:status resp)))))))

;; ---- Form Def --------------------------------------------------------------

(defn- create-form-def
  ([study id] (create-form-def study id "name-182856"))
  ([study id name] (api/create-form-def (connect) study id name))
  ([study id name desc]
   (api/create-form-def (connect) study id name {:description desc})))

(defn- refresh-form-def [form-def]
  (-> (refresh-study (:study/_forms form-def))
      (find-form-def (:form-def/id form-def))))

(deftest find-form-def-handler-test
  (-> (create-study "s-183549")
      (create-form-def "id-224127"))

  (let [req {:request-method :get
             :headers {"accept" "application/edn"}
             :params {:study-id "s-183549" :form-def-id "id-224127"}
             :db (d/db (connect))}
        resp ((find-form-def-handler path-for) req)]

    (is (= 200 (:status resp)))

    (testing "Body contains self link"
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :form-def-handler (:handler (:href self-link))))
        (is (= [:study-id "s-183549" :form-def-id "id-224127"]
               (:args (:href self-link))))))

    (testing "Response contains an ETag"
      (is (= "\"d6c752651c6ece3c991b7df3946b3495\""
             (get-in resp [:headers "ETag"]))))))

(deftest form-def-handler-test
  (-> (create-study "s-183549")
      (create-form-def "id-224127"))

  (let [req {:request-method :get
             :headers {"accept" "application/edn"}
             :params {:study-id "s-183549" :form-def-id "id-224127"}
             :db (d/db (connect))}
        resp ((find-form-def-handler path-for) req)]

    (is (= 200 (:status resp)))

    (testing "Body contains self link"
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :form-def-handler (:handler (:href self-link))))
        (is (= [:study-id "s-183549" :form-def-id "id-224127"]
               (:args (:href self-link))))))

    (testing "Response contains an ETag"
      (is (= "\"d6c752651c6ece3c991b7df3946b3495\""
             (get-in resp [:headers "ETag"])))))

  (testing "Non-conditional update fails"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :db (d/db (connect))}
          resp ((form-def-handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Require conditional update." (:body resp)))))

  (testing "Update with missing body fails"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :db (d/db (connect))}
          resp ((form-def-handler path-for) req)]
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
          resp ((form-def-handler path-for) req)]
      (is (= 400 (:status resp)))
      (is (= "Invalid request body." (:body resp)))))

  (testing "Update fails on ETag missmatch"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :db (d/db (connect))}
          resp ((form-def-handler path-for) req)]
      (is (= 412 (:status resp)))))

  (testing "Update fails on missing name"
    (let [req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"foo\""}
               :params {:study-id "s-183549" :form-def-id "id-224127"}
               :body (str->is "{}")
               :conn (connect)
               :db (d/db (connect))}
          resp ((form-def-handler path-for) req)]
      (is (= 422 (:status resp)))))

  (testing "Update fails in-transaction on name missmatch"
    (let [form-def (-> (create-study "s-095742")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"e525c38e88c56516bf0e68fd0ba33fea\""}
               :params {:study-id "s-095742" :form-def-id "id-224127"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :conn (connect)
               :db (d/db (connect))}
          _ (api/update-form-def (connect) form-def {:name "name-095717"}
                                 {:name "name-203308"})
          resp ((form-def-handler path-for) req)]
      (is (= 409 (:status resp)))))

  (testing "Update with JSON succeeds"
    (let [form-def (-> (create-study "s-100747")
                       (create-form-def "id-224127" "name-095717"))
          req {:request-method :put
               :headers {"accept" "application/json"
                         "content-type" "application/json"
                         "if-match" "\"26f096525c26dec3f2229c94e58bd384\""}
               :params {:study-id "s-100747" :form-def-id "id-224127"}
               :body (str->is "{\"name\": \"name-202906\"}")
               :conn (connect)
               :db (d/db (connect))}
          resp ((form-def-handler path-for) req)]
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
          resp ((form-def-handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (= "name-143536" (:name (refresh-form-def form-def))))))

  (testing "Update can remove description"
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
          resp ((form-def-handler path-for) req)]
      (is (= 204 (:status resp)))
      (is (nil? (:description (refresh-form-def form-def)))))))

(deftest create-form-def-handler-test
  (create-study "s-100937")

  (testing "Create without id and name fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937"}
               :conn (connect)}]
      (is (= 422 (:status ((create-form-def-handler nil) req))))))

  (testing "Create without name fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224305"}
               :conn (connect)}]
      (is (= 422 (:status ((create-form-def-handler nil) req))))))

  (testing "Create with id and name only"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224211"
                        :name "name-224240"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-form-def-handler path-for) req)]
      (is (= 201 (:status resp)))
      (let [location (edn/read-string (get-in resp [:headers "Location"]))]
        (is (= [:study-id "s-100937" :form-def-id "id-224211"]
               (:args location))))
      (is (nil? (:body resp)))))

  (testing "Create with description"
    (let [req {:request-method :post
               :params {:study-id "s-100937"
                        :id "id-224401"
                        :name "name-224330"
                        :description "description-224339"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-form-def-handler path-for) req)]
      (is (= 201 (:status resp)))
      (let [form-def (-> (find-study "s-100937") (find-form-def "id-224401"))]
        (is (= "description-224339" (:description form-def))))))

  (testing "Create with existing id fails"
    (let [req {:request-method :post
               :params {:study-id "s-100937" :id "id-224401"
                        :name "name-224439"}
               :conn (connect)
               :db (d/db (connect))}
          resp ((create-form-def-handler path-for) req)]
      (is (= 409 (:status resp))))))

;; ----------------------------------------------------------------------------

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
