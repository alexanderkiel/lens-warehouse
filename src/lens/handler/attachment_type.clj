(ns lens.handler.attachment-type
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [lens.api :as api]
            [lens.util :as util]
            [lens.handler.util :as hu]
            [schema.core :as s]))

(defn path [path-for attachment-type]
  (path-for :attachment-type-handler :eid (hu/entity-id attachment-type)))

(defn link [path-for attachment-type]
  {:href (path path-for attachment-type)
   :label (:attachment-type/id attachment-type)})

(defnk render [attachment-type [:request path-for]]
  {:data
   {:id (:attachment-type/id attachment-type)}

   :links
   {:up {:href (path-for :service-document-handler)}
    :self (link path-for attachment-type)}

   :ops #{:delete}})

(def handler
  "Handler for GET and DELETE on a attachment-type."
  (resource
    (hu/entity-resource-defaults)

    :allowed-methods [:get :delete]

    :exists? (hu/exists? :attachment-type)

    :etag
    (hu/etag 1)

    :delete!
    (fnk [conn attachment-type]
      (api/retract-entity conn (:db/id attachment-type)))

    :handle-ok render))

(defn render-embedded [path-for attachment-type]
  {:data
   {:id (:attachment-type/id attachment-type)}
   :links
   {:self (link path-for attachment-type)}})

(defn render-embedded-list-xf [path-for]
  (map (partial render-embedded path-for)))

(defn render-create-form [path-for]
  {:href (path-for :create-attachment-type-handler)
   :params
   {:id {:type s/Str :desc "The unique id of an attachment type."}}})

(defnk render-list [db [:request path-for]]
  {:links
   {:up {:href (path-for :service-document-handler)}
    :self {:href (path-for :all-attachment-types-handler)}}
   :forms
   {:lens/create-attachment-type
    (render-create-form path-for)}
   :embedded
   {:lens/attachment-types
    (->> (api/all-attachment-types db)
         (into [] (render-embedded-list-xf path-for)))}})

(def all-handler
  (resource
    (hu/resource-defaults)

    ;;TODO: ETag

    :handle-ok render-list))

(def ^:private CreateParamSchema
  {:id util/NonBlankStr
   s/Any s/Any})

(def create-handler
  (resource
    (hu/create-resource-defaults)

    :processable? (hu/validate-params CreateParamSchema)

    :post!
    (fnk [conn [:request [:params id]]]
      (if-let [attachment-type (api/create-attachment-type conn id)]
        {:attachment-type attachment-type}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [attachment-type [:request path-for]]
      (path path-for attachment-type))

    :handle-exception
    (hu/duplicate-exception "The attachment-type exists already.")))
