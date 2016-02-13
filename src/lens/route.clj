(ns lens.route
  (:require [schema.core :as s]))

(def ContextPath
  #"^(?:/[^/]+)*$")

(defn eid-handler [handler]
  {[:eid] handler})

(s/defn routes [context-path :- ContextPath]
  [context-path
   [[(if (= "" context-path) "/" "") :service-document-handler]

    ["/p/"
     [["id" :item-def-profile-handler]
      ["igd" :item-group-def-profile-handler]
      ["fd" :form-def-profile-handler]
      ["sed" :study-event-def-profile-handler]
      ["s" :study-profile-handler]
      ["it" :inquiry-type-profile-handler]]]

    ["/id/" (eid-handler :item-def-handler)]

    ["/ir/" (eid-handler :item-ref-handler)]

    ["/s/"
     (eid-handler
       [["/fid" :find-item-def-handler]
        ["/figd" :find-item-group-def-handler]
        ["/ffd" :find-form-def-handler]
        ["/fsed" :find-study-event-def-handler]

        ["" :study-handler]

        ["/study-event-defs"
         {"" :create-study-event-def-handler
          ["/page/" :page-num] :study-study-event-defs-handler}]

        ["/form-defs"
         {"" :create-form-def-handler
          ["/page/" :page-num] :study-form-defs-handler}]

        ["/item-group-defs"
         {"" :create-item-group-def-handler
          ["/page/" :page-num] :study-item-group-defs-handler}]

        ["/item-defs"
         {"" :create-item-def-handler
          ["/page/" :page-num] :study-item-defs-handler}]

        ["/subjects"
         {"" :create-subject-handler}]])]

    ["/item-group-def/"
     (eid-handler
       [["/find-item-ref" :find-item-ref-handler]
        ["" :item-group-def-handler]
        ["/item-refs"
         {"" :create-item-ref-handler
          ["/page/" :page-num] :item-refs-handler}]])]

    ["/item-group-ref/" (eid-handler :item-group-ref-handler)]

    ["/form-def/"
     (eid-handler
       {"" :form-def-handler
        "/find-item-group-ref" :find-item-group-ref-handler
        "/item-group-refs"
        {"" :create-item-group-ref-handler
         ["/page/" :page-num] :item-group-refs-handler}})]

    ["/form-ref/" (eid-handler :form-ref-handler)]

    ["/study-event-def/"
     (eid-handler
       {"" :study-event-def-handler
        "/find-form-ref" :find-form-ref-handler
        "/form-refs"
        {"" :create-form-ref-handler
         ["/page/" :page-num] :form-refs-handler}})]

    ["/studies"
     {"" :create-study-handler
      ["/page/" :page-num] :all-studies-handler}]

    [["/code-lists" :id] :code-list-handler]

    ["/snapshots"
     {"" :all-snapshots-handler
      ["/" :id] :snapshot-handler
      ["/" :id "/query"] :query-handler}]

    ["/most-recent-snapshot" :most-recent-snapshot-handler]
    ["/find-study" :find-study-handler]
    ["/basis-t" :basis-t-handler]
    ["/health" :health-handler]

    ["/inquiry-types"
     {"" {:get :all-inquiry-types-handler
          :post :create-inquiry-type-handler}
      ["/" :eid] :inquiry-type-handler}]

    ["/attachment-types"
     {"" {:get :all-attachment-types-handler
          :post :create-attachment-type-handler}
      ["/" :eid] :attachment-type-handler}]]])
