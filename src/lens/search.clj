(ns lens.search
  (:use plumbing.core)
  (:require [clojure.core.async :refer [<!!]]
            [async-error.core :refer [<??]]
            [lens.logging :refer [error info]]
            [datomic.api :as d]
            [schema.core :as s :refer [Str Int]]
            [lens.bus :as bus]
            [lens.search.api :as api]
            [com.stuartsierra.component :refer [Lifecycle]]))

(def ^:private form-def-pull-pattern
  [:db/id
   {:study/_form-defs [:study/id]}
   :form-def/id
   :form-def/name
   :form-def/desc
   :form-def/keywords
   :form-def/recording-type])

(defn- contains-form-def-attr? [db {:keys [a]}]
  (= "form-def" (namespace (:db/ident (d/entity db a)))))

(s/defn ^:private ingester [conn :- api/Conn]
  (fnk ingest [db-after tx-data]
    (doseq [[eid tx-data] (group-by :e tx-data)]
      (when (some #(contains-form-def-attr? db-after %) tx-data)
        (let [form-def (d/pull db-after form-def-pull-pattern eid)]
          (when-let [id (:form-def/id form-def)]
            (let [eid (:db/id form-def)
                  study-id (:study/id (:study/_form-defs form-def))
                  form-def (-> form-def
                               (dissoc :db/id :study/_form-defs)
                               (assoc :study-id study-id))
                  res (<!! (api/index conn :form-def (str eid) form-def))]
              (if (instance? Throwable res)
                (error res {:msg (format "Error while ingesting form def %s: %s"
                                         id (.getMessage res))
                            :ex-data (ex-data res)})
                (info {:type :ingest :sub-type :form-def :study-id study-id
                       :id id})))))))))

(s/defrecord SearchIndexIngestor [conn :- api/Conn bus token]
  Lifecycle
  (start [ingestor]
    (assoc ingestor :token (bus/listen-on bus :tx-report (ingester conn))))
  (stop [ingestor]
    (bus/unlisten bus token)
    (assoc ingestor :token nil)))

(defn new-search-index-ingestor []
  (map->SearchIndexIngestor {}))

(def ^:private index-config
  {:settings
   {:number_of_shards 1
    :analysis
    {:analyzer
     {:default
      {:type :custom
       :tokenizer :standard
       :filter [:lowercase :german_normalization :trigram]}
      :form-def-id
      {:type :custom
       :tokenizer :ngram-36
       :filter [:lowercase]}}
     :tokenizer
     {:ngram-36
      {:type :nGram
       :min_gram 3
       :max_gram 6}}
     :filter
     {:lowercase
      {:type :lowercase}
      :trigram
      {:type :nGram
       :min_gram 3
       :max_gram 3}}}}
   :mappings
   {:form-def
    {:properties
     {:id {:type :string :analyzer :form-def-id}
      :study-id {:type :string :index :not_analyzed}
      :name {:type :string}
      :desc {:type :string}
      :recording-type {:type :string :index :not_analyzed}}}}})

(s/defrecord SearchConn [host :- Str port :- Int index :- Str]
  Lifecycle
  (start [conn]
    (when-not (api/index-exists? conn)
      (<?? (api/create-index conn index-config)))
    conn)
  (stop [conn]
    conn))

(defn new-search-conn [host port index]
  (->SearchConn host port index))
