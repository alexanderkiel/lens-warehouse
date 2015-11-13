(ns lens.search.api
  (:use plumbing.core)
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [schema.core :as s :refer [Str Int]]
            [clojure.java.io :as io]
            [cemerick.url :refer [url url-encode]]
            [clojure.string :as str]
            [lens.util :as util])
  (:import [java.net URI]))

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
       :body (json/write-str m)
       :as :stream}
      (callback ch process-create-index-resp))
    ch))

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
       :body (json/write-str m)
       :as :stream}
      (callback ch process-index-resp))
    ch))

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
       :body (json/write-str query)
       :as :stream
       :start (System/nanoTime)}
      (callback ch process-search-resp))
    ch))

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
       :body (json/write-str {:text text})
       :as :stream}
      (callback ch process-analyze-resp))
    ch))

(comment

  (<!! (search {:host "192.168.99.100" :index "lens"} :form-def
               {:_source [:id]
                :query
                {:bool
                 {:filter
                  {:term {:study-id "S002"}}
                  :must
                  {:simple_query_string
                   {:query "t0000"
                    :fields [:id :name :desc :keywords :recording-type]
                    :flags "AND|NOT|PHRASE"}}}}}))

  (<!! (analyze {:host "192.168.99.100" :index "lens"} :desc
                "Die Daten wurden bei Probanden der ADULT-Kohorte im Altersbereich 18-79 Jahre erhoben.\n                        Im Rahmen eines standardisierten Interviews wurden verschiedene soziodemographische Angaben wie z.B. Nationalität, Familienstand, Schul- und Berufsausbildung sowie Angaben zur Erwerbstätigkeit des Studienteilnehmers sowie des Lebenspartners erfasst."))

  (<!! (analyze {:host "192.168.99.100" :index "lens"} :id "T00001"))
  (<!! (analyze {:host "192.168.99.100" :index "lens"} :recording-type "Interview"))

  )
