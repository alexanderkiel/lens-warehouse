(ns lens.core
  (:require [clojure.tools.cli :as cli]
            [org.httpkit.server :refer [run-server]]
            [lens.app :refer [app]]
            [lens.util :refer [parse-int]]
            [clojure.string :as str]))

(def cli-options
  [["-p" "--port PORT" "Listen on this port"
    :default 8080
    :parse-fn parse-int
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-i" "--ip IP" "The IP to bind"
    :default "0.0.0.0"]
   ["-t" "--thread NUM" "Number of worker threads"
    :default 4
    :parse-fn parse-int
    :validate [#(< 0 % 64) "Must be a number between 0 and 64"]]
   ["-d" "--db-uri URI" "The Datomic database URI to use"
    :validate [#(.startsWith % "datomic")
               "Database URI has to start with datomic."]]
   ["-c" "--context-path PATH"
    "An optional context path under which the workbook service runs. Has to start and end with a slash."
    :default "/"]
   ["-h" "--help" "Show this help"]])

(defn usage [options-summary]
  (->> ["Usage: lens-workbook [options]"
        ""
        "Options:"
        options-summary
        ""]
       (str/join "\n")))

(defn error-msg [errors]
  (str/join "\n" errors))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        version (System/getProperty "lens-warehouse.version")]
    (cond
      (:help options)
      (exit 0 (usage summary))

      errors
      (exit 1 (error-msg errors))

      (nil? (:db-uri options))
      (exit 1 "Missing database URI."))

    (let [{:keys [db-uri context-path]} options]
      (run-server (app (assoc options :version version))
                  (merge {:worker-name-prefix "http-kit-worker-"} options))
      (println "Version:" version)
      (println "Max Memory:" (quot (.maxMemory (Runtime/getRuntime))
                                   (* 1024 1024)) "MB")
      (println "Num CPUs:" (.availableProcessors (Runtime/getRuntime)))
      (println "Datomic:" db-uri)
      (println "Context Path:" context-path)
      (println "Server started")
      (println "Listen at" (str (:ip options) ":" (:port options)))
      (println "Using" (:thread options) "worker threads"))))
