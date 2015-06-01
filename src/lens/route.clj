(ns lens.route)

(defn routes [context-path]
  [(if (= "/" context-path) "" context-path)
   {(if (= "/" context-path) "/" "") :service-document-handler

    "/studies"
    {"" :create-study-handler
     ["/page/" :page-num] :all-studies-handler
     ["/" :study-id] :study-handler

     "/study-event-defs"
     {"" :study-event-defs-handler
      ["/" :study-event-def-id] :study-event-def-handler}

     "/form-defs"
     {"" :create-form-def-handler
      ["/page/" :page-num] :study-form-defs-handler
      ["/" :id] :form-def-handler
      ["/" :id "/count"] :form-count-handler
      ["/" :id "/search-item-groups"] :search-item-groups-handler}

     "/find-form-def" :find-form-def-handler

     "/subjects"
     {"" :create-subject-handler
      ["/" :subject-id] {:get :subject-handler
                         :delete :delete-subject-handler}}}
    "/item-groups"
    {"" :all-item-groups-handler
     ["/" :id] :item-group-handler
     ["/" :id "/count"] :item-group-count-handler
     ["/" :id "/search-items"] :search-items-handler}
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
    "/find-item-group" :find-item-group-handler
    "/find-item" :find-item-handler}])
