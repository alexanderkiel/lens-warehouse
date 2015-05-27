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
            [lens.api :as api])
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

;; ---- Subject ---------------------------------------------------------------

(deftest get-subject-handler-test
  (testing "Body contains self link"
    (api/create-subject (connect) "id-181341")
    (let [req {:request-method :get
               :headers {"accept" "application/edn"}
               :params {:id "id-181341"}
               :db (d/db (connect))}
          resp ((get-subject-handler path-for) req)]
      (is (= 200 (:status resp)))
      (let [self-link (:self (:links (edn/read-string (:body resp))))]
        (is (= :get-subject-handler (:handler (:href self-link))))
        (is (= [:id "id-181341"] (:args (:href self-link))))))))

(deftest create-subject-handler-test
  (testing "Create without id fails"
    (let [req {:request-method :post
               :params {}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))

  (testing "Create with invalid sex fails"
    (let [req {:request-method :post
               :params {:id "id-172029"
                        :sex "foo"}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))

  (testing "Create with invalid birth-date fails"
    (let [req {:request-method :post
               :params {:id "id-172029"
                        :birth-date "foo"}
               :conn (connect)}]
      (is (= 422 (:status ((create-subject-handler nil) req))))))

  (testing "Create with id only"
    (let [path-for (fn [_ _ id] id)
          req {:request-method :post
               :params {:id "id-165339"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= "id-165339" (get-in resp [:headers "Location"])))
      (is (nil? (:body resp)))))

  (testing "Create with sex"
    (let [req {:request-method :post
               :params {:id "id-171917"
                        :sex "male"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= :subject.sex/male
             (:subject/sex (api/subject (d/db (connect)) "id-171917"))))))

  (testing "Create with birth-date"
    (let [req {:request-method :post
               :params {:id "id-173133"
                        :birth-date "2015-05-25"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 201 (:status resp)))
      (is (= (tc/to-date (t/date-time 2015 5 25))
             (:subject/birth-date (api/subject (d/db (connect)) "id-173133"))))))

  (testing "Create with existing id fails"
    (api/create-subject (connect) "id-182721")
    (let [req {:request-method :post
               :params {:id "id-182721"}
               :conn (connect)}
          resp ((create-subject-handler path-for) req)]
      (is (= 409 (:status resp))))))

;; ---- Study -----------------------------------------------------------------

(deftest study-handler-test
  (letfn [(study [id] (api/study (d/db (connect)) id))
          (create-study
            ([id name] (api/create-study (connect) id name))
            ([id name desc]
             (api/create-study (connect) id name {:description desc})))]

    (testing "Body contains self link"
      (create-study "id-224127" "name-224123")
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
            _ (api/update-study! (connect) "id-202032" {:name "name-202034"}
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
        (is (= "name-202906" (:name (study "id-203855"))))))

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
        (is (= "name-143536" (:name (study "id-143317"))))))

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
        (is (nil? (:description (study "id-143317"))))))))

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
             (:description (api/study (d/db (connect)) "id-224401"))))))

  (testing "Create with existing id fails"
    (api/create-study (connect) "id-224419" "name-224431")
    (let [req {:request-method :post
               :params {:id "id-224419" :name "name-224439"}
               :conn (connect)}
          resp ((create-study-handler path-for) req)]
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
