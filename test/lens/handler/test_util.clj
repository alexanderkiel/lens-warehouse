(ns lens.handler.test-util
  (:require [lens.test-util :refer :all]
            [datomic.api :as d]
            [lens.api :as api]))

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
