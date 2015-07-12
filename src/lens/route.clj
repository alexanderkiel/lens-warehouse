(ns lens.route)

(defn routes [context-path]
  [(if (= "/" context-path) "" context-path)
   {(if (= "/" context-path) "/" "") :service-document-handler

    ["/study/" :eid]
    {"" :study-handler
     "/find-study-event-def" :find-study-event-def-handler
     "/find-form-def" :find-form-def-handler
     "/find-item-group-def" :find-item-group-def-handler
     "/find-item-def" :find-item-def-handler

     "/study-event-defs"
     {"" :create-study-event-def-handler
      ["/page/" :page-num] :study-study-event-defs-handler}

     "/form-defs"
     {"" :create-form-def-handler
      ["/page/" :page-num] :study-form-defs-handler}

     "/item-group-defs"
     {"" :create-item-group-def-handler
      ["/page/" :page-num] :study-item-group-defs-handler}

     "/item-defs"
     {"" :create-item-def-handler
      ["/page/" :page-num] :study-item-defs-handler}

     "/subjects"
     {"" :create-subject-handler}}

    ["/study-event-def/" :eid]
    {"" :study-event-def-handler
     "/find-form-ref" :find-form-ref-handler
     "/form-refs"
     {"" :create-form-ref-handler
      ["/page/" :page-num] :form-refs-handler
      ["/" :form-def-id] :form-ref-handler}}

    ["/form-ref/" :eid]
    {"" :form-ref-handler}

    ["/form-def/" :eid]
    {"" :form-def-handler}

    ["/item-group-def/" :eid]
    {"" :item-group-def-handler}

    ["/item-def/" :eid]
    {"" :item-def-handler}

    "/studies"
    {"" :create-study-handler
     ["/page/" :page-num] :all-studies-handler}

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
