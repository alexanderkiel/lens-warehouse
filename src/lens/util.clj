(ns lens.util
  (:require [clojure.core.async :as async :refer [go go-loop <! <!! >!! close!
                                                  alts! alts!! alt!!]]
            [clojure.core.cache :as cache]
            [clojure.core.reducers :as r]
            [datomic.api :as d])
  (:import [datomic Entity]
           [java.util.concurrent ExecutionException]))

(def uuid-regexp #"[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}")

;; ---- Core-Async ------------------------------------------------------------

(defn throw-err
  "Throws x if throwable returns it otherwise."
  [x]
  (when (instance? Throwable x) (throw x))
  x)

(defmacro <?
  "Like <! but throws throwable values instead of returning them."
  [ch]
  `(throw-err (<! ~ch)))

(defmacro <??
  "Like <?? but throws throwable values instead of returning them."
  [ch]
  `(throw-err (<!! ~ch)))

(defn fan-out
  "Takes an input channel and puts its values to many output channels.

  If a number of output channels is given they are created and closed after the
  input channel is closed. Directly supplied output channels are not closed."
  [in cs-or-n]
  (let [cs (if (number? cs-or-n)
             (repeatedly cs-or-n async/chan)
             cs-or-n)]
    (go-loop []
      (if-let [x (<! in)]
        (let [outs (mapv #(vector % x) cs)]
          (alts! outs)
          (recur))
        (when (number? cs-or-n)
          (doseq [out cs]
            (close! out)))))
    cs))

(defmacro catch-err
  "Wraps body in a try catch block which just returns the error.

  Useable in conjunction with channels where errors are conveyed instead of
  being thrown."
  [& body]
  `(try ~@body (catch Throwable e# e#)))

(defn process
  "Processes (f x) in a separate thread until the input channel closes.

  Returns a channel conveying the results. Catches errors thrown by f and puts
  them on the result channel."
  [f ch]
  (let [out (async/chan)]
    (async/thread
      (loop []
        (if-let [v (<!! ch)]
          (do
            (>!! out (catch-err (f v)))
            (recur))
          (close! out))))
    out))

(defn process-parallel
  "Processes (f x) in n separate threads until the input channel closes.

  Returns a channel conveying the results. Catches errors thrown by f and puts
  them on the result channel."
  [f n ch]
  {:pre [(number? n) (pos? n)]}
  (->> (fan-out ch n)
       (map #(process f %))
       (async/merge)))

(defn spool [coll]
  (let [out (async/chan)]
    (async/thread
      (reduce
        (fn [_ v] (>!! out v))
        nil
        coll)
      (close! out))
    out))

(defn expired? [timeout]
  (alt!! timeout true :default false))

(defmacro try-until
  "Returns the result of evaluating expr or nil when the timeout expires before.

  Can be used to set an upper limit on expression evaluation time. When the
  timeout is already expired, the expression evaluation is not started. When the
  timeout expires before the expression evaluation is finished, the result will
  be discarded.

  Can be used in conjunction with a cache where the expression evaluation is
  usually very fast due to caching and sometimes slow due to cache misses but
  it is important not to block for a long time in the case of a cache miss."
  [timeout expr]
  `(when-not (expired? ~timeout)
     (first (alts!! [~timeout (go ~expr)]))))

;; ---- Timer -----------------------------------------------------------------

(defn duration
  "Returns the duaration in milliseconds from a System/nanoTime start point."
  [start]
  (/ (double (- (System/nanoTime) start)) 1000000.0))

(defmacro timer [m & body]
  `(let [start# (System/nanoTime)
         ret# ~@body]
     (println (pr-str (merge {:type :timer :duration (duration start#)} ~m)))
     ret#))

(defn paginate
  "Returns a reducible collection of all items belonging to one page.

  Page numbers start with one."
  [page-size page coll]
  (->> coll
       (r/drop (* (dec page) page-size))
       (r/take page-size)))

(defmacro retry [cnt & body]
  `(loop [n# ~cnt]
     (let [res# (try ~@body (catch Exception e# e#))]
       (if (instance? Throwable res#)
         (if (pos? n#)
           (recur (dec n#))
           (throw res#))
         res#))))

(defn unwrap-execution-exception [e]
  (if (instance? ExecutionException e)
    (.getCause e)
    e))

(defn error-type
  "Returns the error type of exceptions from transaction functions or nil."
  [e]
  (:type (ex-data (unwrap-execution-exception e))))

(defn parse-long [s]
  (Long/parseLong s))

;; ---- Datomic ---------------------------------------------------------------

(defn entity?
  "Test if x is a Datomic entity."
  [x]
  (instance? Entity x))

(defmacro update-cache!
  "Updates a cache stored in an atom. Returns the updated cache.

  Value expression is only evaluated on a cache miss."
  [cache-atom key value-expr]
  `(swap! ~cache-atom #(if (cache/has? % ~key)
                        (cache/hit % ~key)
                        (cache/miss % ~key ~value-expr))))

(defn create
  "Submits a transaction which creates an entity.

  The fn is called with a temp id from partition and should return the tx-data
  which is submitted.

  Returns the created entity."
  [conn partition fn]
  (let [tid (d/tempid partition)
        tx-result @(d/transact conn (fn tid))
        db (:db-after tx-result)]
    (d/entity db (d/resolve-tempid db (:tempids tx-result) tid))))
