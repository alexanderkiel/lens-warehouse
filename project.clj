(defproject lens-warehouse "0.1-SNAPSHOT"
  :description "The data warehouse component of the Lens system."
  :url "https://github.com/alexanderkiel/lens-warehouse"
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.371"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.reader "0.9.2"]
                 [prismatic/plumbing "0.4.4"]
                 [http-kit "2.1.18"]
                 [org.clojars.akiel/ring-hap "0.3"]
                 [bidi "1.20.2"]
                 [pathetic "0.5.1"]
                 [liberator "0.13"]
                 [org.clojars.akiel/shortid "0.1.1"]
                 [org.clojars.akiel/digest "0.1"]
                 [clj-time "0.6.0"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [ch.qos.logback/logback-classic "1.1.2"]
                 [com.stuartsierra/component "0.3.0"]
                 [environ "1.0.1"]
                 [org.clojars.akiel/async-error "0.1"
                  :exclusions [org.clojure/clojurescript]]]

  :profiles {:dev [:datomic-free :dev-common :base :system :user :provided]
             :dev-pro [:datomic-pro :dev-common :base :system :user :provided]

             :dev-common
             {:source-paths ["dev"]
              :dependencies [[org.clojure/tools.namespace "0.2.4"]
                             [criterium "0.4.3"]
                             [juxt/iota "0.2.0"]]
              :global-vars {*print-length* 20}}

             :datomic-free
             {:dependencies [[com.datomic/datomic-free "0.9.5327"
                              :exclusions [org.slf4j/slf4j-nop commons-codec
                                           com.amazonaws/aws-java-sdk
                                           joda-time]]]}

             :datomic-pro
             {:repositories [["my.datomic.com" "https://my.datomic.com/repo"]]
              :dependencies [[com.datomic/datomic-pro "0.9.5327"
                              :exclusions [org.slf4j/slf4j-nop
                                           org.slf4j/slf4j-log4j12
                                           org.apache.httpcomponents/httpclient
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
