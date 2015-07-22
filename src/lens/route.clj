(ns lens.route)

(defn eid-handler [handler]
  {[:eid] handler})

(defn routes [context-path]
  (assert (re-matches #"/(?:.*[^/])?" context-path))
  [(if (= "/" context-path) "" context-path)
   [["/item-def/" (eid-handler :item-def-handler)]

    ["/item-ref/" (eid-handler :item-ref-handler)]

    ["/study/"
     (eid-handler
       [["/find-item-def" :find-item-def-handler]
        ["/find-item-group-def" :find-item-group-def-handler]
        ["/find-form-def" :find-form-def-handler]
        ["/find-study-event-def" :find-study-event-def-handler]

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

    ["/study-profile" :study-profile-handler]
    ["/study-event-def-profile" :study-event-def-profile-handler]
    ["/form-def-profile" :form-def-profile-handler]
    ["/item-group-def-profile" :item-group-def-profile-handler]
    ["/item-def-profile" :item-def-profile-handler]

    [(if (= "/" context-path) "/" "") :service-document-handler]]])
