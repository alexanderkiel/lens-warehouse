(ns lens.api-test
  (:require [clojure.test :refer :all]
            [lens.api :as api :refer [find-subject find-form-def]]
            [lens.schema :refer [load-base-schema]]
            [datomic.api :as d]
            [clojure.core.reducers :as r]))

(defn- connect [] (d/connect "datomic:mem:test"))

(defn database-fixture [f]
  (do
    (d/create-database "datomic:mem:test")
    (load-base-schema (connect)))
  (f)
  (d/delete-database "datomic:mem:test"))

(use-fixtures :each database-fixture)

;; ---- Study -----------------------------------------------------------------

(defn- study [id]
  (api/find-study (d/db (connect)) id))

(defn- create-study
  ([] (create-study "s-134745"))
  ([id] (create-study id "name-134748"))
  ([id name & [more]] (api/create-study (connect) id name more)))

(defn- update-study [id old-props new-props]
  (api/update-study (connect) id old-props new-props))

(defn- refresh-study [study]
  (api/find-study (d/db (connect)) (:study/id study)))

(deftest study-test
  (create-study "id-221714")

  (testing "is found"
    (is (:db/id (study "id-221714"))))

  (testing "is not found"
    (is (nil? (study "other-id-221735")))))

(deftest create-study-test
  (testing "create with id and name only"
    (let [study (create-study "id-221752" "name-222227")]
      (is (= "id-221752" (:study/id study)))
      (is (= "name-222227" (:name study)))))

  (testing "create with id, name and description"
    (let [study (create-study "id-222745" "name-222227"
                              {:description "description-222413"})]
      (is (= "description-222413" (:description study)))))

  (testing "create with existing id fails"
    (create-study "id-221808")
    (is (not (create-study "id-221808")))))

(deftest update-study-test
  (create-study "id-195829" "name-195834")

  (testing "not found"
    (is (= :not-found (update-study "id-200608" {:name "name-195834"}
                                    {:name "name-195920"}))))

  (testing "conflict"
    (is (= :conflict (update-study "id-195829" {:name "name-200730"}
                                   {:name "name-195920"}))))

  (testing "update with matching name"
    (is (nil? (update-study "id-195829" {:name "name-195834"}
                            {:name "name-195920"})))
    (is (= "name-195920" (:name (study "id-195829")))))

  (create-study "id-170349" "name-195834")

  (testing "update can add description"
    (is (nil? (api/update-study (connect) "id-170349" {:name "name-195834"}
                                {:name "name-195920"
                                 :description "desc-164555"})))
    (is (= "desc-164555" (:description (study "id-170349")))))

  (create-study "id-162717" "name-162720" {:description "desc-162727"})

  (testing "update can remove description"
    (is (nil? (update-study "id-162717" {:name "name-162720"
                                         :description "desc-162727"}
                            {:name "name-162720"})))
    (is (nil? (:description (study "id-195829"))))))

;; ---- Subject ---------------------------------------------------------------

(defn- create-subject
  ([study] (create-subject study "sub-141405"))
  ([study id] (api/create-subject (connect) study id)))

(defn- retract-subject [subject]
  (api/retract-subject (connect) subject))

(deftest find-subject-test
  (let [study (create-study)
        _ (create-subject study "id-142055")
        study (refresh-study study)]

    (testing "is found"
      (is (:db/id (find-subject study "id-142055"))))

    (testing "is not found"
      (is (nil? (find-subject study "other-id-142447"))))))

(deftest create-subject-test
  (let [study (create-study)]

    (testing "create with id only"
      (is (= "id-142850" (:subject/id (create-subject study "id-142850")))))

    (testing "create with existing id fails"
      (create-subject study "id-143440")
      (is (nil? (create-subject study "id-143440"))))))

(deftest retract-subject-test
  (let [study (create-study)
        subject (create-subject study "id-170655")]

    (retract-subject subject)
    (is (nil? (find-subject study "id-170655")))))

;; ---- Form Def --------------------------------------------------------------

(defn- create-form-def
  ([study id] (create-form-def study id "name-212214"))
  ([study id name & [more]] (api/create-form-def (connect) study id name more)))

(defn- update-form-def [form-def old-props new-props]
  (api/update-form-def (connect) form-def old-props new-props))

(defn- refresh-form-def [form-def]
  (-> (refresh-study (:study/_form-defs form-def))
      (find-form-def (:form-def/id form-def))))

(deftest find-form-def-test
  (let [study (create-study)
        _ (create-form-def study "id-221714")
        study (refresh-study study)]

    (testing "is found"
      (is (:form-def/id (find-form-def study "id-221714"))))

    (testing "is not found"
      (is (nil? (find-form-def study "other-id-221735"))))))

(deftest create-form-def-test
  (let [study (create-study)]

    (testing "create with id and name only"
      (let [form-def (create-form-def study "id-221752" "name-222227")]
        (is (= "id-221752" (:form-def/id form-def)))
        (is (= "name-222227" (:name form-def)))))

    (testing "create with id, name and description"
      (let [form-def (create-form-def study "id-222745" "name-222227"
                                      {:description "desc-222413"})]
        (is (= "desc-222413" (:description form-def)))))

    (testing "create with existing id fails"
      (create-form-def study "id-221808")
      (is (not (create-form-def study "id-221808"))))))

(deftest update-form-def-test
  (let [study (create-study)]

    (testing "not found"
      (let [form-def (create-form-def study "id-195829" "name-195834")]
        (d/transact (connect) [[:db.fn/retractEntity (:db/id form-def)]])
        (is (= :not-found (update-form-def form-def {} {})))))

    (testing "conflict"
      (let [form-def (create-form-def study "id-195829" "name-195834")]
        (is (= :conflict (update-form-def form-def {:name "name-200730"} {})))))

    (testing "update with matching name"
      (let [form-def (create-form-def study "id-214924" "name-195834")]
        (is (nil? (update-form-def form-def {:name "name-195834"}
                                   {:name "name-195920"})))
        (is (= "name-195920" (:name (refresh-form-def form-def))))))

    (testing "update can add description"
      (let [form-def (create-form-def study "id-215227" "name-195834")]
        (is (nil? (update-form-def form-def {:name "name-195834"}
                                   {:name "name-195920"
                                    :description "desc-164555"})))
        (is (= "desc-164555" (:description (refresh-form-def form-def))))))

    (testing "update can remove description"
      (let [form-def (create-form-def study "id-162717" "name-162720"
                                          {:description "desc-162727"})]
        (is (nil? (update-form-def form-def {:name "name-162720"
                                                 :description "desc-162727"}
                                       {:name "name-162720"})))
        (is (nil? (:description (refresh-form-def form-def))))))))

;; ---- Item Group ------------------------------------------------------------

#_(deftest item-group-test
  (let [conn (connect)]
    (api/create-form-def conn "f-184846" "name-184720")
    @(d/transact conn [[:add-item-group "ig-185204" "f-184846"]])
    (testing "is found"
      (is (:db/id (api/item-group (d/db conn) "ig-185204"))))
    (testing "is not found"
      (is (nil? (api/item-group (d/db conn) "other-ig-185411"))))))

#_(deftest item-test
  (let [conn (connect)]
    (api/create-form-def conn "f-184846" "name-184720")
    @(d/transact conn [[:add-item-group "ig-185204" "f-184846"]])
    @(d/transact conn [[:add-item "i-185700" "ig-185204"
                        :data-point/long-value]])
    (testing "is found"
      (is (:db/id (api/item (d/db conn) "i-185700"))))
    (testing "is not found"
      (is (nil? (api/item (d/db conn) "other-i-185730"))))))

#_(deftest code-list-item-test
  (let [conn (connect)]
    (api/create-form-def conn "f-184846" "name-184720")
    @(d/transact conn [[:add-item-group "ig-185204" "f-184846"]])
    @(d/transact conn [[:add-item "i-185700" "ig-185204"
                        :data-point/long-value]])
    @(d/transact conn [[:add-code-list "cl-190356" :code-list-item/long-code]])
    @(d/transact conn [[:add-code-list-to-item "i-185700" "cl-190356"]])
    @(d/transact conn [[:add-code-list-item "cl-190356"
                        :code-list-item/long-code 185935]])
    (testing "is found"
      (is (:db/id (api/code-list-item (api/item (d/db conn) "i-185700") 185935))))
    (testing "is not found"
      (is (nil? (api/code-list-item (api/item (d/db conn) "i-185700") 185947))))))

(deftest all-studies-test
  (let [conn (connect)]
    (testing "with zero studies"
      (is (= #{} (->> (api/all-studies (d/db conn))
                      (r/map :study/id)
                      (into #{})))))
    (testing "with one study"
      (api/create-study conn "id-144922" "name-225358")
      (is (= #{"id-144922"} (->> (api/all-studies (d/db conn))
                                 (r/map :study/id)
                                 (into #{})))))
    (testing "with two studies"
      (api/create-study conn "id-150211" "name-225425")
      (is (= #{"id-144922"
               "id-150211"} (->> (api/all-studies (d/db conn))
                                 (r/map :study/id)
                                 (into #{})))))))

;; ---- Study Event Stats -----------------------------------------------------

(defn- create-study-event-def [study]
  (api/create-study-event-def (connect) study "sed-142027" "name-142054"))

(defn- create-study-event [subject study-event-def]
  (api/create-study-event (connect) subject study-event-def))

(defn- create-form
  ([study-event form-def]
   (api/create-form (connect) study-event form-def))
  ([study-event form-def repeat-key]
   (api/create-form (connect) study-event form-def repeat-key)))

(deftest num-form-def-study-events-test
  (let [study (create-study)
        study-event-def (create-study-event-def study)
        subject (create-subject study)
        study-event (create-study-event subject study-event-def)]
    (testing "without form repetition"
      (let [form-def (create-form-def study "fd")
            _ (create-form study-event form-def)
            form-def (refresh-form-def form-def)]
        (is (= 1 (api/num-form-def-study-events form-def)))))
    (testing "with form repetition"
      (let [form-def (create-form-def study "fdr" "n" {:repeating true})
            _ (create-form study-event form-def "1")
            _ (create-form study-event form-def "2")
            form-def (refresh-form-def form-def)]
        (is (= 1 (api/num-form-def-study-events form-def)))))))

;; ---- Data Stats ------------------------------------------------------------

(deftest num-forms-test
  (let [study (create-study)
        study-event-def (create-study-event-def study)
        subject (create-subject study)
        study-event (create-study-event subject study-event-def)]
    (testing "without form repetition"
      (let [form-def (create-form-def study "fd")
            _ (create-form study-event form-def)
            form-def (refresh-form-def form-def)]
        (is (= 1 (api/num-forms form-def)))))
    (testing "with form repetition"
      (let [form-def (create-form-def study "fdr" "n" {:repeating true})
            _ (create-form study-event form-def "1")
            _ (create-form study-event form-def "2")
            form-def (refresh-form-def form-def)]
        (is (= 2 (api/num-forms form-def)))))))

;; ---- Query -----------------------------------------------------------------

(deftest clean-atom-test
  (are [orig cleaned] (= cleaned (api/clean-atom orig))
                      nil nil
                      [] nil
                      [:invalid] nil
                      [:form] nil
                      [:form :a] {:type :form :id :a}
                      [:form :a :b] {:type :form :id :a}))

(deftest query-disjunction-test
  (testing "nil disjunction"
    (is (= #{} (api/query-disjunction nil nil))))
  (testing "empty disjunction"
    (is (= #{} (api/query-disjunction nil [])))))

(deftest query-conjunction-test
  (testing "nil disjunction"
    (is (= #{} (api/query-conjunction nil nil))))
  (testing "empty disjunction"
    (is (= #{} (api/query-conjunction nil [])))))

(deftest nearest-rank-test
  (testing "Percentile smaller 0"
    (is (thrown? AssertionError (api/nearest-rank -1 1))))
  (testing "Percentile bigger 1"
    (is (thrown? AssertionError (api/nearest-rank 2 1))))
  (testing "n = 0"
    (is (thrown? AssertionError (api/nearest-rank 0.5 0)))))
