(ns lens.route)

(defn routes [context-path]
  [(if (= "/" context-path) "" context-path)
   {(if (= "/" context-path) "/" "") :service-document-handler

    "/studies"
    {"" :create-study-handler

     ["/page/" :page-num] :all-studies-handler

     ["/" :study-id]
     {"" :study-handler

      "/find-study-event-def" :find-study-event-def-handler

      "/study-event-defs"
      {"" :create-study-event-def-handler
       ["/page/" :page-num] :study-study-event-defs-handler
       ["/" :study-event-def-id]
       {"" :study-event-def-handler

        "/find-form-ref" :find-form-ref-handler

        "/form-refs"
        {"" {:get :form-refs-handler
             :post :create-form-ref-handler}
         ["/" :form-def-id] :form-ref-handler}}}

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

      "/find-item-def" :find-item-def-handler

      "/item-defs"
      {"" :create-item-def-handler
       ["/page/" :page-num] :study-item-defs-handler
       ["/" :item-def-id] :item-def-handler
       ["/" :item-def-id "/count"] :item-count-handler
       ["/" :item-def-id "/search-items"] :search-items-handler}

      "/subjects"
      {"" :create-subject-handler
       ["/" :subject-id] {:get :subject-handler
                          :delete :delete-subject-handler}}}}

    ["/code-lists" :id] :code-list-handler
    "/snapshots"
    {"" :all-snapshots-handler
     ["/" :id] :snapshot-handler
     ["/" :id "/query"] :query-handler}
    "/most-recent-snapshot" :most-recent-snapshot-handler
    "/find-study" :find-study-handler

    "/study-profile" :study-profile-handler
    "/study-event-def-profile" :study-event-def-profile-handler
    "/form-def-profile" :form-def-profile-handler
    "/item-group-def-profile" :item-group-def-profile-handler
    "/item-def-profile" :item-def-profile-handler}])
