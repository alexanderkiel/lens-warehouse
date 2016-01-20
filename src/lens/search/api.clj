(ns lens.search.api
  (:use plumbing.core)
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [schema.core :as s :refer [Str Int]]
            [clojure.java.io :as io]
            [cemerick.url :refer [url]]
            [clojure.string :as str]
            [lens.util :as util])
  (:import [java.net URI]))

;; ---- Schemas ---------------------------------------------------------------

(def Representation
  {s/Any s/Any})

(def Response
  {s/Any s/Any})

(def ProcessFn
  "A function which processes a response."
  (s/=> Representation Response))

(def Conn
  "A Search API connection.

  :host  - the Elastic Search host
  :port  - the Elastic Search port (defaults to 9200)
  :index - the Elastic Search index"
  {:host Str
   (s/optional-key :port) Int
   :index Str})

(def SearchResult
  {:hits
   {:total Int
    :hits
    [{:_id Str}]}})

;; ---- Helper ----------------------------------------------------------------

(s/defn ^:private callback [ch process-fn :- ProcessFn]
  (fn [resp]
    (try
      (async/put! ch (process-fn resp))
      (catch Throwable t (async/put! ch t)))
    (async/close! ch)))

(defnk ^:private parse-response [[:headers content-type] :as resp]
  (case (first (str/split content-type #";"))
    "application/json"
    (update resp :body #(json/read (io/reader %) :key-fn keyword))
    "text/plain"
    (update resp :body slurp)
    resp))

(defn- error-ex-data [opts error]
  {:error error :uri (URI/create (:url opts))})

(defn- status-ex-data [opts status body]
  {:status status :uri (URI/create (:url opts)) :body body})

(defnk ^:private base-uri :- Str [host {port 9200} index]
  (str "http://" host ":" port "/" index))

;; ---- Index Exists ----------------------------------------------------------

(defn index-exists-error-ex-info [opts error]
  (ex-info (str "Error while looking whether the index at " (:url opts)
                " exists: " error)
           (error-ex-data opts error)))

(defn- index-exists-status-ex-info [opts status body]
  (ex-info (str "Unexpected status " status " while looking whether the index "
                "at " (:url opts) " exists")
           (status-ex-data opts status body)))

(defn- process-index-exists-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (index-exists-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 true
      404 false
      (throw (index-exists-status-ex-info opts status body)))))

(s/defn index-exists?
  "Returns a channel conveying true if the index exists."
  [conn :- Conn]
  (let [ch (async/chan)]
    (http/request
      {:url (base-uri conn)
       :method :head
       :as :stream}
      (callback ch process-index-exists-resp))
    ch))

;; ---- Create Index ----------------------------------------------------------

(defn create-index-error-ex-info [opts error]
  (ex-info (str "Error while creating an index at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- create-index-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while creating an index at "
                (:url opts))
           (status-ex-data opts status body)))

(defn- process-create-index-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (create-index-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 body
      (throw (create-index-status-ex-info opts status body)))))

(s/defn create-index [conn :- Conn m]
  (let [ch (async/chan)]
    (http/request
      {:url (base-uri conn)
       :method :put
       :body (json/write-str m :escape-unicode false)
       :as :stream}
      (callback ch process-create-index-resp))
    ch))

;; ---- Delete Index ----------------------------------------------------------

(defn delete-index-error-ex-info [opts error]
  (ex-info (str "Error while deleting an index at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- delete-index-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while deleting an index at "
                (:url opts))
           (status-ex-data opts status body)))

(defn- process-delete-index-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (delete-index-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 body
      (throw (delete-index-status-ex-info opts status body)))))

(s/defn delete-index [conn :- Conn]
  (let [ch (async/chan)]
    (http/request
      {:url (base-uri conn)
       :method :delete
       :as :stream}
      (callback ch process-delete-index-resp))
    ch))

;; ---- Index Status ----------------------------------------------------------

(defn index-status-error-ex-info [opts error]
  (ex-info (str "Error while fetching the index status at " (:url opts) ": "
                error)
           (error-ex-data opts error)))

(defn- index-status-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while fetching the index status at "
                (:url opts))
           (status-ex-data opts status body)))

(defn- process-index-status-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (index-status-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 body
      (throw (index-status-status-ex-info opts status body)))))

(s/defn index-status [conn :- Conn]
  (let [ch (async/chan)]
    (http/request
      {:url (str (base-uri conn) "/_status")
       :method :get
       :as :stream}
      (callback ch process-index-status-resp))
    ch))

;; ---- Index -----------------------------------------------------------------

(defn index-error-ex-info [opts error]
  (ex-info (str "Error while indexing a document at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- index-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while indexing a document at "
                (:url opts))
           (status-ex-data opts status body)))

(defn- process-index-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (index-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      (200 201) body
      (throw (index-status-ex-info opts status body)))))

(s/defn index [conn :- Conn type :- s/Keyword id :- Str m]
  (assert (not (str/blank? id)))
  (let [ch (async/chan)]
    (http/request
      {:url (str (url (base-uri conn) (name type) id))
       :method :put
       :body (json/write-str m :escape-unicode false)
       :as :stream}
      (callback ch process-index-resp))
    ch))

;; ---- Search ----------------------------------------------------------------

(defn- search-error-ex-info [opts error]
  (ex-info (str "Error while searching at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- search-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while searching at " (:url opts))
           (status-ex-data opts status body)))

(defn- process-search-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (search-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 (assoc body :took-overall (util/duration (:start opts)))
      (throw (search-status-ex-info opts status body)))))

(s/defn search [conn :- Conn type :- s/Keyword query :- {s/Any s/Any}]
  "Returns a channel conveying the SearchResult."
  (let [ch (async/chan)]
    (http/request
      {:url (str (url (base-uri conn) (name type) "_search"))
       :method :get
       :body (json/write-str query :escape-unicode false)
       :as :stream
       :start (System/nanoTime)}
      (callback ch process-search-resp))
    ch))

;; ---- Search ----------------------------------------------------------------

(defn- explain-error-ex-info [opts error]
  (ex-info (str "Error while explaining at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- explain-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while explaining at " (:url opts))
           (status-ex-data opts status body)))

(defn- process-explain-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (explain-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 (assoc body :took-overall (util/duration (:start opts)))
      (throw (explain-status-ex-info opts status body)))))

(s/defn explain [conn :- Conn type :- s/Keyword id :- Str
                 query :- {s/Any s/Any}]
  "Returns a channel conveying the ExplainResult."
  (let [ch (async/chan)]
    (http/request
      {:url (str (url (base-uri conn) (name type) id "_explain"))
       :method :get
       :body (json/write-str query :escape-unicode false)
       :as :stream
       :start (System/nanoTime)}
      (callback ch process-explain-resp))
    ch))

;; ---- Analyze ---------------------------------------------------------------

(defn- analyze-error-ex-info [opts error]
  (ex-info (str "Error while analyzing at " (:url opts) ": " error)
           (error-ex-data opts error)))

(defn- analyze-status-ex-info [opts status body]
  (ex-info (str "Non-ok status " status " while analyzing at " (:url opts))
           (status-ex-data opts status body)))

(defn- process-analyze-resp [{:keys [opts error status] :as resp}]
  (when error
    (throw (analyze-error-ex-info opts error)))
  (letk [[body] (parse-response resp)]
    (case status
      200 (mapv :token (:tokens body))
      (throw (analyze-status-ex-info opts status body)))))

(s/defn analyze [conn :- Conn field :- s/Keyword text :- Str]
  "Returns a channel conveying the AnalyzeResult."
  (let [ch (async/chan)]
    (http/request
      {:url (-> (url (base-uri conn) "_analyze")
                (assoc :query {:field (name field)})
                (str))
       :method :get
       :body (json/write-str text :escape-unicode false)
       :as :stream}
      (callback ch process-analyze-resp))
    ch))

(comment

  (<!! (index-exists? {:host "192.168.99.100" :index "lens"}))
  (<!! (index-status {:host "192.168.99.100" :index "lens"}))

  (<!! (search {:host "192.168.99.100" :index "lens"} :form-def
               {:size 4
                :_source [:id :name]
                :query
                {:filtered
                 {:filter
                  {:term {:study-id "S001"}}
                  :query
                  {:simple_query_string
                   {:query "schlaf"
                    :fields [:id :name :desc :keywords :recording-type]
                    :flags "AND|NOT|PHRASE"}}}}}))

  (<!! (explain {:host "192.168.99.100" :index "lens"} :form-def "580542139466928"
                {:query
                 {:filtered
                  {:filter
                   {:term {:study-id "S003"}}
                   :query
                   {:simple_query_string
                    {:query "stress"
                     :fields [:id :name :desc :keywords :recording-type]
                     :flags "AND|NOT|PHRASE"}}}}}))

  (<!! (create-index
         {:host "192.168.99.100" :index "test6"}
         {:settings
          {:number_of_shards 1
           :analysis
           {:analyzer
            {:german-plain
             {:type :custom
              :tokenizer :standard
              :filter [:lowercase]}
             :german-norm
             {:type :custom
              :tokenizer :standard
              :filter [:lowercase :german_normalization]}
             :german-tri
             {:type :custom
              :tokenizer :standard
              :filter [:lowercase :german_normalization :trigram]}}
            :filter
            {:lowercase
             {:type :lowercase}
             :trigram
             {:type :nGram
              :min_gram 3
              :max_gram 3}}}}
          :mappings
          {:test
           {:properties
            {:id {:type :string :analyzer :german-plain}
             :id1 {:type :string :analyzer :german-norm}
             :id2 {:type :string :analyzer :german-tri}
             :id3 {:type :string :analyzer :german}}}}}))

  (<!! (analyze {:host "192.168.99.100" :index "lens"} :desc
                "Die Daten wurden bei Probanden der ADULT-Kohorte im Altersbereich 18-79 Jahre erhoben.\n                        Im Rahmen eines standardisierten Interviews wurden verschiedene soziodemographische Angaben wie z.B. Nationalität, Familienstand, Schul- und Berufsausbildung sowie Angaben zur Erwerbstätigkeit des Studienteilnehmers sowie des Lebenspartners erfasst."))

  (<!! (analyze {:host "192.168.99.100" :index "lens"} :name "hän"))
  (<!! (analyze {:host "192.168.99.100" :index "test6"} :id2 "händigkeit"))

  (json/write-str {:id "hän"} :escape-unicode false)

  )
