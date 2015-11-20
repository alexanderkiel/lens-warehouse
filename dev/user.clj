(ns user
  (:use plumbing.core)
  (:use criterium.core)
  (:require [clojure.pprint :refer [pprint pp]]
            [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :refer [refresh]]
            [datomic.api :as d]
            [schema.core :as s]
            [com.stuartsierra.component :as comp]
            [lens.system :refer [new-system]]
            [lens.util :as util]
            [lens.search :as search]
            [environ.core :refer [env]]
            [lens.api :as api]))

(s/set-fn-validation! true)

(def system nil)

(defn init []
  (assert (nil? system))
  (alter-var-root #'system (constantly (new-system env))))

(defn start []
  (alter-var-root #'system comp/start))

(defn stop []
  (alter-var-root #'system comp/stop))

(defn startup []
  (init)
  (start)
  (println "Server running at port" (:port system)))

(defn reset []
  (stop)
  (refresh :after 'user/startup))

(defn connect []
  (d/connect (:db-uri system)))

;; Init Development
(comment
  (startup)
  )

;; Reset after making changes
(comment
  (reset)
  )

;; Connection and Database in the REPL
(comment
  (def conn (connect))
  (def db (d/db conn))
  )

;; (Re)Index all form defs
(comment
  (doseq [study (api/all-studies (d/db (connect)))
          form-def (:study/form-defs study)]
    (search/index-form-def (:search-conn system) form-def))
  )
