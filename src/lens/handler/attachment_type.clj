(ns lens.handler.attachment-type
  (:use plumbing.core)
  (:require [clojure.core.reducers :as r]
            [liberator.core :refer [resource]]
            [lens.api :as api]
            [lens.util :as util]
            [lens.handler.util :as hu]
            [lens.reducers :as lr]
            [schema.core :as s]))

(defn all-attachment-types-path
  ([path-for] (all-attachment-types-path path-for 1))
  ([path-for page-num]
   (path-for :all-attachment-types-handler :page-num page-num)))

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

(defn render-embedded-attachment-type [path-for attachment-type]
  {:data
   {:id (:attachment-type/id attachment-type)
    :name (:attachment-type/name attachment-type)
    :desc (:attachment-type/desc attachment-type)}
   :links
   {:self (link path-for attachment-type)}})

(defn render-embedded-attachment-types [path-for attachment-types]
  (r/map #(render-embedded-attachment-type path-for %) attachment-types))

(def ListParamSchema
  {:page-num util/PosInt
   s/Any s/Any})

(defn render-create-attachment-type-form [path-for]
  {:href (path-for :create-attachment-type-handler)
   :params
   {:id {:type s/Str :desc "The unique id of an attachment type."}}})

(def all-attachment-types-handler
  (resource
    (hu/resource-defaults)

    :processable? (hu/coerce-params ListParamSchema)

    ;;TODO: ETag

    :handle-ok
    (fnk [db [:request path-for [:params page-num]]]
      (let [attachment-types (api/all-attachment-types db)
            next-page? (not (lr/empty? (hu/paginate (inc page-num)
                                                    attachment-types)))
            path #(all-attachment-types-path path-for %)]
        {:links
         (-> {:up {:href (path-for :service-document-handler)}
              :self {:href (path page-num)}}
             (hu/assoc-prev page-num path)
             (hu/assoc-next next-page? page-num path))
         :forms
         {:lens/create-attachment-type
          (render-create-attachment-type-form path-for)}
         :embedded
         {:lens/attachment-types
          (->> (hu/paginate page-num attachment-types)
               (render-embedded-attachment-types path-for)
               (into []))}}))))

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
    (hu/duplicate-exception "Attachment-type exists already.")))
