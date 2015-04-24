(ns user
  (:use plumbing.core)
  (:use criterium.core)
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.pprint :refer [pprint pp]]
            [clojure.repl :refer :all]
            [clojure.test :as test]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.core.reducers :as r]
            [clojure.core.cache :as cache]
            [clojure.core.async :as async :refer [chan go go-loop <! <!! >! alts! close!]]
            [lens.schema :as schema]
            [lens.routes :as routes]
            [lens.api :as api :refer :all]
            [datomic.api :as d]
            [system]
            [lens.util :as util]))

(def system nil)

(defn init []
  (alter-var-root #'system (constantly (system/system (system/env)))))

(defn start []
  (alter-var-root #'system system/start))

(defn stop []
  (alter-var-root #'system system/stop))

(defn startup []
  (init)
  (start)
  (println "Server running at port" (:port system)))

(defn reset []
  (stop)
  (refresh :after 'user/startup))

(comment
  (startup)
  (reset))

(defn delete-database []
  (d/delete-database (:db-uri system)))

(defn create-database []
  (d/create-database (:db-uri system)))

(defn connect []
  (d/connect (:db-uri system)))

(defn study-event-col? [col]
  (and (= "Prolog" (:group-name col))
       (#{"gruppe" "grp"} (some-> (:alias col) (str/lower-case)))))

(defn count-datoms [db]
  (->> (d/datoms db :eavt)
       (r/map (constantly 1))
       (reduce +)))

(comment
  (time (count-datoms db)))

(comment
  (value-quartiles (item (d/db (connect)) "T00002_F0014"))
  (value-histogram (item (d/db (connect)) "T00002_F0014"))
  (pst)
  )

(defn same-item-group? [[i1 i2]]
  (= (:item/item-group i1) (:item/item-group i2)))

(defn a2? [[i1 i2]]
  (and
    (contains? (set (map :study/id (-> i1 :item/item-group :item-group/form :form/studies))) "A2")
    (contains? (set (map :study/id (-> i2 :item/item-group :item-group/form :form/studies))) "A2")))

(defn show-item-pair [[i1 i2]]
  [(:item/question i1)
   (:item/id i1)
   (:item/id i2)])

(defn common-question? [[q]]
  (re-find #"Erfassender|Erfassungsdatum|Bemerkungen|Uhrzeit Ende" q))

(comment
  (def conn (connect))
  (def db (d/db conn))
  (def res1 (d/q '[:find ?i1 ?i2
                   :where
                   [?i1 :item/question ?q1]
                   [?i2 :item/question ?q2]
                   [(< ?i1 ?i2)]
                   [(= ?q1 ?q2)]] db))
  (count res)

  (->> (d/q '[:find [?id ...]
              :where
              [?s :study/id "A2"]
              [?f :form/studies ?s]
              [?f :form/id ?id]] db)
       (sort)
       (last))

  (->> res
       (map #(mapv (partial d/entity db) %))
       (remove same-item-group?)
       (filter a2?)
       (map show-item-pair)
       (remove common-question?)
       (group-by first)
       (map-vals #(->> (map rest %) (flatten) (set) (sort)))
       (sort-by first)
       (map (fn [[q items]] (str/join "," (cons (str \" q \") items))))
       (str/join "\n")
       (spit "duplicates.csv"))
  )
