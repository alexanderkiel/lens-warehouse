(ns lens.reducers
  (:require [clojure.core.reducers :as r])
  (:refer-clojure :exclude [empty? partition-by count]))

(defn partition-by
  "Applies f to each value in coll, splitting it each time f returns
  a new value."
  [f coll]
  (reify
    clojure.core.protocols/CollReduce
    (coll-reduce [this f1]
      (clojure.core.protocols/coll-reduce this f1 (f1)))
    (coll-reduce [_ f1 init]
      (let [result (clojure.core.protocols/coll-reduce
                     coll
                     (fn [accum v]
                       (let [r (f v)
                             vs (:vs accum)]
                         (cond
                           (= r (:last accum))
                           (assoc! accum :vs (conj! vs v))

                           (nil? vs)
                           (-> accum
                               (assoc! :vs (transient [v]))
                               (assoc! :last r))

                           :else
                           (let [ret (f1 (:ret accum) (persistent! vs))]
                             (if (reduced? ret)
                               (reduced {:ret @ret})
                               (assoc! accum :ret ret
                                       :vs (transient [v])
                                       :last r))))))
                     (transient {:ret init}))]
        (if (:vs result)
          (f1 (:ret result) (persistent! (:vs result)))
          (:ret result))))))

(defn empty? [coll]
  (zero? (reduce + (r/take 1 (r/map (constantly 1) coll)))))

(defn count [coll]
  (reduce + (r/map (constantly 1) coll)))
