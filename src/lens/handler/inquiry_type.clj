(ns lens.handler.inquiry-type
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [lens.logging :refer [debug]]
            [lens.api :as api]
            [lens.util :as util]
            [lens.handler.util :as hu]
            [schema.core :as s]))

(defn path [path-for inquiry-type]
  (path-for :inquiry-type-handler :eid (hu/entity-id inquiry-type)))

(defn link [path-for inquiry-type]
  {:href (path path-for inquiry-type)
   :label (:inquiry-type/name inquiry-type)})

(def select-props
  (hu/select-props :inquiry-type :name :desc :keywords :inquiry-type))

(defnk render [inquiry-type [:request path-for]]
  {:data
   {:id (:inquiry-type/id inquiry-type)
    :name (:inquiry-type/name inquiry-type)
    :rank (:inquiry-type/rank inquiry-type)}

   :links
   {:up {:href (path-for :service-document-handler)}
    :self (link path-for inquiry-type)
    :profile {:href (path-for :inquiry-type-profile-handler)}}

   :ops #{:update :delete}})

(def schema
  {:name s/Str
   :rank s/Int})

(def handler
  "Handler for GET, PUT and DELETE on a inquiry-type.

  Implementation note on PUT:

  The resource compares the current ETag with the If-Match header based on a
  possibly old version of the inquiry-type taken from a database outside of the
  transaction. The update transaction is than tried with name and desc
  from that possibly old inquiry-type as reference. The transaction only succeeds if
  the name and desc are still the same on the in-transaction inquiry-type."
  (resource
    (hu/entity-resource-defaults)

    :processable?
    (fnk [db [:request [:params eid]] :as ctx]
      (let [inquiry-type (api/find-entity db :inquiry-type (hu/to-eid eid))]
        ((hu/entity-processable (assoc schema :id (s/eq (:inquiry-type/id inquiry-type)))) ctx)))

    :exists? (hu/exists? :inquiry-type)

    :etag
    (hu/etag #(-> % :inquiry-type :inquiry-type/name)
             #(-> % :inquiry-type :inquiry-type/rank)
             1)

    :put!
    (fnk [conn inquiry-type new-entity]
      (let [new-entity (util/prefix-namespace :inquiry-type new-entity)]
        (debug {:type :update :sub-type :inquiry-type :new-entity new-entity})
        {:update-error (api/update-inquiry-type conn inquiry-type
                                                (select-props inquiry-type)
                                                (select-props new-entity))}))

    :delete!
    (fnk [conn inquiry-type] (api/retract-entity conn (:db/id inquiry-type)))

    :handle-ok render))

(defn render-embedded [path-for inquiry-type]
  (-> {:data
       {:id (:inquiry-type/id inquiry-type)
        :name (:inquiry-type/name inquiry-type)
        :rank (:inquiry-type/rank inquiry-type)}
       :links
       {:self
        (link path-for inquiry-type)}}))

(defn render-embedded-list-xf [path-for]
  (map (partial render-embedded path-for)))

(defn render-create-form [path-for]
  {:href (path-for :create-inquiry-type-handler)
   :params
   {:id {:type s/Str :desc "The unique id of an inquiry-type."}
    :name {:type s/Str :desc "The human readable name of an inquiry-type."}
    :rank {:type s/Int :desc "The rank which forms with this inquiry-type should have in ordering."}}})

(defnk render-list [db [:request path-for]]
  {:links
   {:up {:href (path-for :service-document-handler)}
    :self {:href (path-for :all-inquiry-types-handler)}}

   :forms
   {:lens/create-inquiry-type
    (render-create-form path-for)}

   :embedded
   {:lens/inquiry-types
    (->> (api/all-inquiry-types db)
         (into [] (render-embedded-list-xf path-for)))}})

(def all-handler
  "Resource of all inquiry-types."
  (resource
    (hu/resource-defaults)

    ;;TODO: ETag

    :handle-ok render-list))

(def ^:private CreateParamSchema
  {:id util/NonBlankStr
   :name util/NonBlankStr
   :rank util/PosInt
   s/Any s/Any})

(def create-handler
  (resource
    (hu/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn [:request [:params id name rank]]]
      (if-let [inquiry-type (api/create-inquiry-type conn id name rank)]
        {:inquiry-type inquiry-type}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [inquiry-type [:request path-for]]
      (path path-for inquiry-type))

    :handle-exception
    (hu/duplicate-exception "The inquiry-type exists already.")))
