(ns lens.handler.test-util
  (:require [lens.test-util :refer :all]
            [datomic.api :as d]
            [lens.api :as api]
            [lens.handler.util :as hu]))

(defn find-entity [type eid]
  (let [db (d/db (connect))]
    (api/find-entity db type (hu/to-eid eid))))

(defn find-study [id]
  (api/find-study (d/db (connect)) id))

(defn create-study
  ([id] (create-study id "name-172037"))
  ([id name]
   (create-study id name "desc-171720"))
  ([id name desc]
   (api/create-study (connect) id name desc)))

(defn refresh-study [study]
  (api/find-study (d/db (connect)) (:study/id study)))

(defn create-inquiry-type
  ([id]
    (create-inquiry-type id "time-130144" 130124))
  ([id name]
    (create-inquiry-type id name 130124))
  ([id name rank]
   (api/create-inquiry-type (connect) id name rank)))
