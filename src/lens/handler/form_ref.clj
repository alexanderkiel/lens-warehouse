(ns lens.handler.form-ref
  (:use plumbing.core)
  (:require [liberator.core :refer [resource]]
            [schema.core :as s]
            [lens.handler.util :as hu]
            [lens.handler.study :as study]
            [lens.handler.study-event-def :as hse]))

(defn- form-ref-path
  ([path-for form-ref]
   (form-ref-path path-for (:study/id (:study-event-def/_form-refs form-ref))
                  (:form-ref/id form-ref)))
  ([path-for study-id form-ref-id]
   (path-for :form-ref-handler :study-id study-id :form-ref-id form-ref-id)))

(def AppendParamSchema
  {:study-id s/Str
   :study-event-def-id s/Str
   :form-id s/Str
   s/Any s/Any})

(def append-handler
  (resource
    (hu/standard-create-resource-defaults)

    :processable?
    (fnk [[:request params]]
      (hu/validate AppendParamSchema params))

    :exists? (fn [ctx] (some-> (study/exists? ctx) (hse/exists?)))

    #_:post!
    #_(fnk [conn study-event-def [:request params]]
      (if-let [form-ref (api/create-form-ref conn study-event-def
                                             (:form-id params)
                                             (:order-number params))]
        {:form-ref form-ref}
        (throw (ex-info "Duplicate!" {:type :duplicate}))))

    :location
    (fnk [form-ref [:request path-for]] (form-ref-path path-for form-ref))

    :handle-exception
    (hu/duplicate-exception
      "The form-ref exists already." study/build-up-link)))
