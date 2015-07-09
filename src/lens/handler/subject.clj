(ns lens.handler.subject
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [liberator.core :refer [resource to-location]]
            [lens.api :as api]
            [lens.handler.study :as study]
            [schema.core :as s]
            [lens.handler.util :as hu]))

(defn path [path-for subject]
  (path-for :subject-handler :eid (hu/entity-id subject)))

(defnk render [subject [:request path-for]]
  {:data
   {:id (:subject/id subject)}
   :links
   {:up {:href (study/path path-for (:subject/study subject))}
    :self {:href (path path-for subject)}}})

(def handler
  (resource
    (hu/resource-defaults)

    :exists? (hu/exists? :subject)

    :handle-ok render

    :handle-not-found
    (fnk [[:request path-for]] (hu/error-body path-for "Subject not found."))))

(def CreateParamSchema
  {:id s/Str
   s/Any s/Any})

(def create-handler
  (resource
    (study/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn study [:request [:params id]]]
      (if-let [subject (api/create-subject conn study id)]
        {:subject subject}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [subject [:request path-for]] (path path-for subject))

    :handle-exception
    (study/duplicate-exception "The subject exists already.")))

(def delete-handler
  (fnk [conn [:params eid path-for]]
    (if (api/retract-entity conn eid)
      {:status 204}
      (hu/ring-error path-for 404 "Subject not found."))))
