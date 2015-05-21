(defproject lens-warehouse "0.1-SNAPSHOT"
  :description "The data warehouse component of the Lens system."
  :url "https://github.com/alexanderkiel/lens-warehouse"

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.reader "0.9.2"]
                 [prismatic/plumbing "0.4.3"]
                 [slingshot "0.12.2"]
                 [http-kit "2.1.18"]
                 [ring/ring-core "1.3.2" :exclusions [commons-codec]]
                 [pathetic "0.5.1"]
                 [bidi "1.18.11" :exclusions [org.clojure/clojurescript]]
                 [liberator "0.12.2"]
                 [com.cognitect/transit-clj "0.8.271"]
                 [io.clojure/liberator-transit "0.3.0"]
                 [pandect "0.5.2"]
                 [clj-time "0.6.0"]
                 [clj-stacktrace "0.2.7"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [ch.qos.logback/logback-classic "1.1.2"]]

  :profiles {:dev
             {:source-paths ["dev"]
              :dependencies [[org.clojure/tools.namespace "0.2.4"]
                             [criterium "0.4.3"]
                             [com.datomic/datomic-free "0.9.5173"
                              :exclusions [org.slf4j/slf4j-nop commons-codec
                                           com.amazonaws/aws-java-sdk
                                           joda-time]]
                             [cursive/datomic-stubs "0.9.5153" :scope "provided"]]
              :global-vars {*print-length* 20}}

             :datomic-free
             {:dependencies [[com.datomic/datomic-free "0.9.5173"
                              :exclusions [org.slf4j/slf4j-nop commons-codec
                                           com.amazonaws/aws-java-sdk
                                           joda-time]]]}

             :datomic-pro
             {:repositories [["my.datomic.com" "https://my.datomic.com/repo"]]
              :dependencies [[com.datomic/datomic-pro "0.9.5173"
                              :exclusions [org.slf4j/slf4j-nop
                                           org.slf4j/slf4j-log4j12
                                           org.apache.httpcomponents/httpclient
                                           com.fasterxml.jackson.core/jackson-annotations
                                           com.fasterxml.jackson.core/jackson-core
                                           com.fasterxml.jackson.core/jackson-databind
                                           commons-codec
                                           joda-time]]
                             [com.basho.riak/riak-client "1.4.4"
                              :exclusions [com.fasterxml.jackson.core/jackson-annotations
                                           com.fasterxml.jackson.core/jackson-core
                                           com.fasterxml.jackson.core/jackson-databind
                                           commons-codec]]
                             [org.apache.curator/curator-framework "2.6.0"
                              :exclusions [io.netty/netty log4j org.slf4j/slf4j-log4j12
                                           com.google.guava/guava]]]}

             :production
             {:main lens.core}}

  :repl-options {:welcome (do
                            (println "   Docs: (doc function-name-here)")
                            (println "         (find-doc \"part-of-name-here\")")
                            (println "   Exit: Control+D or (exit) or (quit)")
                            (println "  Start: (startup)")
                            (println "Restart: (reset)"))})
