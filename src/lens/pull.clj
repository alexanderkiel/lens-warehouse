(ns lens.pull
  (:require [schema.core :as s]))

(def AttrName
  s/Keyword)

(def AttrSpec'
  (s/either AttrName (s/eq '*)))

(def MapSpec
  {AttrName [AttrSpec']})

(def AttrSpec
  (s/either AttrName (s/eq '*) MapSpec))

(def Pattern
  [AttrSpec])
