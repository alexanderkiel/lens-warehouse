(ns lens.handler.subject
  (:use plumbing.core)
  (:require [clojure.core.async :refer [timeout]]
            [liberator.core :refer [resource to-location]]
            [lens.api :as api]
            [lens.handler.study :as study]
            [schema.core :as s]
            [lens.handler.util :as hu]))

(defn path [path-for subject]
  (path-for :subject-handler :study-id (:study/id (:subject/study subject))
            :subject-id (:subject/id subject)))

(defnk exists-subject? [study [:request [:params subject-id]]]
  (when-let [subject (api/find-subject study subject-id)]
    {:subject subject}))

(defnk render [subject [:request path-for]]
  {:data
   {:id (:subject/id subject)}
   :links
   {:up {:href (study/path path-for (:subject/study subject))}
    :self {:href (path path-for subject)}}})

(def handler
  (resource
    (hu/resource-defaults)

    :processable?
    (fnk [[:request params]]
      (and (:study-id params) (:subject-id params)))

    :exists? (fn [ctx] (some-> (study/exists? ctx) (exists-subject?)))

    :handle-ok render

    :handle-not-found
    (fnk [[:request path-for]] (hu/error-body path-for "Subject not found."))))

(def CreateParamSchema
  {:study-id s/Str
   :id s/Str
   s/Any s/Any})

(def create-handler
  (resource
    (hu/standard-create-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (hu/validate CreateParamSchema params))

    :exists? study/exists?

    :post!
    (fnk [conn study [:request [:params id]]]
      (if-let [subject (api/create-subject conn study id)]
        {:subject subject}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [subject [:request path-for]] (path path-for subject))

    :handle-exception
    (hu/duplicate-exception "The subject exists already." study/build-up-link)))

(def delete-handler
  (fnk [conn [:params id path-for]]
    (if (api/retract-subject conn id)
      {:status 204}
      (hu/ring-error path-for 404 "Subject not found."))))
