(ns lens.route)

(defn routes [context-path]
  [(if (= "/" context-path) "" context-path)
   {(if (= "/" context-path) "/" "") :service-document-handler
    "/study-events"
    {"" :all-study-events-handler
     ["/" :id] :study-event-handler}
    ["/subjects/" :id] {:get :get-subject-handler
                        :delete :delete-subject-handler}
    "/forms"
    {"" :all-forms-handler
     ["/" :id] :form-handler
     ["/" :id "/count"] :form-count-handler
     ["/" :id "/search-item-groups"] :search-item-groups-handler}
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
    "/find-form" :find-form-handler
    "/find-item-group" :find-item-group-handler
    "/find-item" :find-item-handler}])
