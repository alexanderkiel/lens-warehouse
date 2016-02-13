(ns lens.search
  (:use plumbing.core)
  (:require [clojure.core.async :refer [<!!]]
            [async-error.core :refer [<??]]
            [lens.logging :refer [error info]]
            [datomic.api :as d]
            [schema.core :as s :refer [Str Int]]
            [lens.bus :as bus]
            [lens.api :as api]
            [lens.search.api :as sapi]
            [com.stuartsierra.component :refer [Lifecycle]]))

(def FormDef
  {:study/_form-defs {:study/id Str}
   :form-def/id Str
   :form-def/name Str
   (s/optional-key :form-def/desc) Str
   (s/optional-key :form-def/keywords) [Str]
   (s/optional-key :form-def/inquiry-type) {:inquiry-type/name Str}})

(def form-def-pull-pattern
  [{:study/_form-defs [:study/id]}
   :form-def/id
   :form-def/name
   :form-def/desc
   :form-def/keywords
   {:form-def/inquiry-type [:inquiry-type/name]}])

(defn- contains-form-def-attr? [db {:keys [a]}]
  (= "form-def" (namespace (:db/ident (d/entity db a)))))

(s/defn index-form-def [conn :- sapi/Conn form-def :- FormDef]
  (let [study-id (:study/id (:study/_form-defs form-def))
        id (str study-id "_" (:form-def/id form-def))
        form-def (-> (assoc form-def :study-id study-id)
                     (dissoc :study/_form-defs)
                     (update :form-def/inquiry-type :inquiry-type/name))
        res (<!! (sapi/index conn :form-def id form-def))]
    (if (instance? Throwable res)
      (error res {:msg (format "Error while indexing form-def %s: %s"
                               id (.getMessage res))
                  :form-def form-def
                  :ex-data (ex-data res)})
      (info {:type :index :sub-type :form-def :id id}))))

(s/defn ^:private ingester [conn :- sapi/Conn]
  (fnk ingest [db-after tx-data]
    (doseq [[eid tx-data] (group-by :e tx-data)]
      (when (some #(contains-form-def-attr? db-after %) tx-data)
        (when-let [form-def (d/pull db-after form-def-pull-pattern eid)]
          (index-form-def conn form-def))))))

(s/defrecord SearchIndexIngestor [conn :- sapi/Conn bus token]
  Lifecycle
  (start [ingestor]
    (assoc ingestor :token (bus/listen-on bus :tx-report (ingester conn))))
  (stop [ingestor]
    (bus/unlisten bus token)
    (assoc ingestor :token nil)))

(defn new-search-index-ingestor []
  (map->SearchIndexIngestor {}))

(def index-config
  {:settings
   {:number_of_shards 1
    :analysis
    {:analyzer
     {:trigram
      {:type :custom
       :tokenizer :standard
       :filter [:lowercase :german_normalization :trigram]}
      :form-def-id
      {:type :custom
       :tokenizer :ngram-36
       :filter [:lowercase]}
      :inquiry-type
      {:type :custom
       :tokenizer :keyword
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
      :name
      {:type :string
       :analyzer :german
       :fields
       {:trigrams
        {:type :string
         :analyzer :trigram}}}
      :desc
      {:type :string
       :analyzer :german
       :fields
       {:trigrams
        {:type :string
         :analyzer :trigram}}}
      :keywords
      {:type :string
       :analyzer :german
       :fields
       {:trigrams
        {:type :string
         :analyzer :trigram}}}
      :inquiry-type {:type :string :analyzer :inquiry-type}}}}})

(s/defrecord SearchConn [host :- Str port :- Int index :- Str]
  Lifecycle
  (start [conn]
    (if (<?? (sapi/index-exists? conn))
      (info {:type :skip-index-creation :conn conn})
      (do (<?? (sapi/create-index conn index-config))
          (info {:type :create-index :conn conn})))
    conn)
  (stop [conn]
    conn))

(defn new-search-conn [host port index]
  (->SearchConn host port index))
