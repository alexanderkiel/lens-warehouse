(ns lens.reducers
  (:require [clojure.core.reducers :as r])
  (:refer-clojure :exclude [empty? count]))

(defn empty? [coll]
  (clojure.core/empty? (into [] (r/take 1 coll))))

(defn count [coll]
  (reduce + (r/map (constantly 1) coll)))
