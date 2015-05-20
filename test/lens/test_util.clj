(ns lens.test-util
  (:require [datomic.api :as d]
            [lens.util :as util]))

(defn create-db [schema]
  (let [uri "datomic:mem://test"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (d/transact conn schema)
      (d/db conn))))

(defn id-attr [ident]
  {:db/ident ident
   :db/valueType :db.type/string
   :db/unique :db.unique/identity
   :db/cardinality :db.cardinality/one})

(defn tempid []
  (d/tempid :db.part/user))

(defn resolve-tempid [tx-data tempid]
  (d/resolve-tempid (:db-after tx-data) (:tempids tx-data) tempid))
