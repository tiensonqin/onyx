(ns onyx.log.two-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def peer-opts
  {:inbox-capacity 1000
   :outbox-capacity 1000
   :job-scheduler :onyx.job-scheduler/round-robin})

(def n-peers 10)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(onyx.api/submit-job (:log env)
                     {:workflow [[:a :b] [:b :c]]
                      :catalog []
                      :task-scheduler :onyx.task-scheduler/greedy})

(onyx.api/submit-job (:log env)
                     {:workflow [[:d :e] [:e :f]]
                      :catalog []
                      :task-scheduler :onyx.task-scheduler/greedy})

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:job-scheduler peer-opts)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          counts (map count (mapcat vals (vals (:allocations new-replica))))]
      (when-not (= counts [5 5])
        (recur new-replica)))))

(fact "peers balanced on 2 jobs" true => true)

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)
