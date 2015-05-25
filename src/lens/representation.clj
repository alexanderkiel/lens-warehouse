(ns lens.representation
  (:require [clojure.data.json :as json]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [liberator.representation :refer [render-map-generic]])
  (:import [java.util Date]))

(defn json-date [date]
  (tf/unparse (tf/formatters :basic-date-time) (tc/from-date date)))

(defmethod render-map-generic "application/json" [data _]
  (json/write-str data :value-fn #(if (instance? Date %2) (json-date %2) %2)))
