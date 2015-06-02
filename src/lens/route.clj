(ns lens.route)

(defn routes [context-path]
  [(if (= "/" context-path) "" context-path)
   {(if (= "/" context-path) "/" "") :service-document-handler

    "/studies"
    {"" :create-study-handler

     ["/page/" :page-num] :all-studies-handler

     ["/" :study-id]
     {"" :study-handler

      "/study-event-defs"
      {"" :study-event-defs-handler
       ["/" :study-event-def-id] :study-event-def-handler}

      "/find-form-def" :find-form-def-handler

      "/form-defs"
      {"" :create-form-def-handler
       ["/page/" :page-num] :study-form-defs-handler
       ["/" :form-def-id] :form-def-handler
       ["/" :form-def-id "/count"] :form-count-handler
       ["/" :form-def-id "/search-item-groups"] :search-item-groups-handler}

      "/find-item-group-def" :find-item-group-def-handler

      "/item-group-defs"
      {"" :create-item-group-def-handler
       ["/page/" :page-num] :study-item-group-defs-handler
       ["/" :item-group-def-id] :item-group-def-handler
       ["/" :item-group-def-id "/count"] :item-group-count-handler
       ["/" :item-group-def-id "/search-items"] :search-items-handler}

      "/subjects"
      {"" :create-subject-handler
       ["/" :subject-id] {:get :subject-handler
                          :delete :delete-subject-handler}}}}

    "/items"
    {"" :all-items-handler
     ["/" :id] :item-handler
     ["/" :id "/count"] :item-count-handler
     ["/" :id "/code-list-item/" :code "/count"]
     :item-code-list-item-count-handler}
    ["/code-lists" :id] :code-list-handler
    "/snapshots"
    {"" :all-snapshots-handler
     ["/" :id] :snapshot-handler
     ["/" :id "/query"] :query-handler}
    "/most-recent-snapshot" :most-recent-snapshot-handler
    "/find-study" :find-study-handler
    "/find-item" :find-item-handler}])
