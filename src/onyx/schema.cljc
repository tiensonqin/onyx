(ns onyx.schema
  (:require [schema.core :as s]
            [onyx.information-model :as i]))

(def NamespacedKeyword
  (s/pred (fn [kw]
            (and (keyword? kw)
                 (namespace kw)))
          'keyword-namespaced?))

(def Function
  (s/pred fn? 'fn?))

(def TaskName
  (s/pred (fn [v]
            (and (not= :all v)
                 (not= :none v)
                 (keyword? v)))
          'task-name?))

(defn ^{:private true} edge-two-nodes? [edge]
  (= (count edge) 2))

(def ^{:private true} edge-validator
  (s/->Both [(s/pred vector? 'vector?)
             (s/pred edge-two-nodes? 'edge-two-nodes?)
             [TaskName]]))

(def Workflow
  (s/->Both [(s/pred vector? 'vector?)
             [edge-validator]]))

(def Language
  (s/enum :java :clojure))

(def PosInt 
  (s/pred pos? 'pos?))

(def SPosInt 
  (s/pred (fn [v] (>= v 0)) 'spos?))

(defn build-allowed-key-ns [nspace]
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= (name nspace) 
                        (namespace k)))))
          'unsupported-key-combination))

(def UnsupportedTaskMapKey
  (build-allowed-key-ns :onyx))

(defn deprecated [key-seq]
  (s/pred
   (fn [_]
     (throw (ex-info (:deprecation-doc (get-in i/model key-seq)) {})))))

(def base-task-map
  {:onyx/name TaskName
   :onyx/type (s/enum :input :output :function)
   :onyx/batch-size PosInt
   (s/optional-key :onyx/params) [s/Any]
   (s/optional-key :onyx/uniqueness-key) s/Any
   (s/optional-key :onyx/deduplicate?) s/Bool
   (s/optional-key :onyx/restart-pred-fn)
   (deprecated [:catalog-entry :model :onyx/restart-pred-fn])
   (s/optional-key :onyx/language) Language
   (s/optional-key :onyx/batch-timeout) PosInt
   (s/optional-key :onyx/doc) s/Str
   (s/optional-key :onyx/bulk?) s/Bool
   (s/optional-key :onyx/max-peers) PosInt
   (s/optional-key :onyx/min-peers) PosInt
   (s/optional-key :onyx/n-peers) PosInt
   (s/optional-key :onyx/required-tags) [s/Keyword]
   UnsupportedTaskMapKey s/Any})

(def FluxPolicy 
  (s/enum :continue :kill :recover))

(defn valid-min-peers-max-peers-n-peers? [entry]
  (case (:onyx/flux-policy entry)
    :continue 
    (or (:onyx/n-peers entry)
        (:onyx/min-peers entry)
        (= (:onyx/max-peers entry) 1))
    :kill 
    (or (:onyx/n-peers entry)
        (:onyx/min-peers entry)
        (= (:onyx/max-peers entry) 2))
    :recover
    (or (:onyx/n-peers entry)
        (and (:onyx/max-peers entry)
             (= (:onyx/max-peers entry) 
                (:onyx/min-peers entry)))
        (= (:onyx/max-peers entry) 1))))

(def FluxPolicyNPeers
  (s/pred valid-min-peers-max-peers-n-peers? 'valid-flux-policy-min-max-n-peers))

(def partial-grouping-task
  {(s/optional-key :onyx/group-by-key) s/Any
   (s/optional-key :onyx/group-by-fn) NamespacedKeyword
   :onyx/flux-policy FluxPolicy})

(defn grouping-task? [task-map]
  (and (#{:function :output} (:onyx/type task-map))
       (or (not (nil? (:onyx/group-by-key task-map)))
           (not (nil? (:onyx/group-by-fn task-map))))))

(def partial-input-task
  {:onyx/plugin (s/either NamespacedKeyword s/Keyword)
   :onyx/medium s/Keyword
   :onyx/type (s/enum :input)
   (s/optional-key :onyx/fn) (s/either NamespacedKeyword s/Keyword)
   (s/optional-key :onyx/input-retry-timeout) PosInt 
   (s/optional-key :onyx/pending-timeout) PosInt 
   (s/optional-key :onyx/max-pending) PosInt})

(def partial-output-task
  {:onyx/plugin (s/either NamespacedKeyword s/Keyword)
   :onyx/medium s/Keyword
   :onyx/type (s/enum :output)
   (s/optional-key :onyx/fn) (s/either NamespacedKeyword s/Keyword)})

(def NonNamespacedKeyword 
  (s/pred (fn [v]
            (and (keyword? v)
                 (not (namespace v))))
          'keyword-non-namespaced))

(def partial-java-plugin
  {:onyx/plugin NonNamespacedKeyword
   (s/optional-key :onyx/fn) s/Keyword})

(def partial-clojure-plugin
  {:onyx/plugin NamespacedKeyword
   (s/optional-key :onyx/fn) NamespacedKeyword})

(def partial-fn-task
  {:onyx/fn (s/either NamespacedKeyword s/Keyword)
   (s/optional-key :onyx/plugin) (s/either NamespacedKeyword s/Keyword)})

(def partial-clojure-fn-task
  {:onyx/fn NamespacedKeyword})

(def partial-java-fn-task
  {:onyx/fn s/Keyword})

(defn java? [task-map]
  (= :java (:onyx/language task-map)))

(def OutputTaskSchema 
  (let [base-output-schema (merge base-task-map partial-output-task)
        base-output-grouping (merge base-output-schema partial-grouping-task)
        java-output-grouping (merge base-output-grouping partial-java-plugin)
        clojure-output-grouping (merge base-output-grouping partial-clojure-plugin)]
    (s/conditional grouping-task?
                   (s/conditional java? 
                                  (s/->Both [FluxPolicyNPeers java-output-grouping])
                                  :else
                                  (s/->Both [FluxPolicyNPeers clojure-output-grouping]))
                   :else 
                   (s/conditional java?
                                  (merge base-output-schema partial-java-plugin)
                                  :else
                                  (merge base-output-schema partial-clojure-plugin)))))

(def InputTaskSchema 
  (let [base-input-schema (merge base-task-map partial-input-task)] 
    (s/conditional java?
                   (merge base-input-schema partial-java-plugin)
                   :else
                   base-input-schema)))

(def FunctionTaskSchema
  (let [base-function-task (merge base-task-map partial-fn-task)]
    (s/conditional grouping-task?
                   (s/conditional java?
                                  (s/->Both [FluxPolicyNPeers (merge base-function-task
                                                                     partial-grouping-task
                                                                     partial-java-fn-task)])
                                  :else
                                  (s/->Both [FluxPolicyNPeers (merge base-function-task
                                                                     partial-grouping-task
                                                                     partial-clojure-fn-task)]))
                   :else
                   (s/conditional java?
                                  (merge base-function-task partial-java-fn-task)
                                  :else
                                  (merge base-function-task partial-clojure-fn-task)))))

(def TaskMap
  (s/conditional #(= (:onyx/type %) :input) 
                 InputTaskSchema
                 #(= (:onyx/type %) :output)
                 OutputTaskSchema
                 #(= (:onyx/type %) :function)
                 FunctionTaskSchema))

(def Catalog
  [TaskMap])

(def Lifecycle
  {:lifecycle/task s/Keyword
   :lifecycle/calls NamespacedKeyword
   (s/optional-key :lifecycle/doc) s/Str
   s/Any s/Any})

(def LifecycleCall
  {(s/optional-key :lifecycle/doc) s/Str
   (s/optional-key :lifecycle/start-task?) Function
   (s/optional-key :lifecycle/before-task-start) Function
   (s/optional-key :lifecycle/before-batch) Function
   (s/optional-key :lifecycle/after-read-batch) Function
   (s/optional-key :lifecycle/after-batch) Function
   (s/optional-key :lifecycle/after-task-stop) Function
   (s/optional-key :lifecycle/after-ack-segment) Function
   (s/optional-key :lifecycle/after-retry-segment) Function
   (s/optional-key :lifecycle/handle-exception) Function})

(def FlowAction
  (s/enum :retry))

(def UnsupportedFlowKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "flow" (namespace k)))))
          'unsupported-flow-key))

(def FlowCondition
  {:flow/from s/Keyword
   :flow/to (s/either s/Keyword [s/Keyword])
   :flow/predicate (s/either s/Keyword [s/Any])
   (s/optional-key :flow/post-transform) NamespacedKeyword
   (s/optional-key :flow/thrown-exception?) s/Bool
   (s/optional-key :flow/action) FlowAction
   (s/optional-key :flow/short-circuit?) s/Bool
   (s/optional-key :flow/exclude-keys) [s/Keyword]
   (s/optional-key :flow/doc) s/Str
   UnsupportedFlowKey s/Any})

(def Unit
  [(s/one s/Int "unit-count")
   (s/one s/Keyword "unit-type")])

(def WindowType
  (s/enum :fixed :sliding :global :session))

(def UnsupportedWindowKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "window" (namespace k)))))
          'unsupported-window-key))

(def Window
  {:window/id s/Keyword
   :window/task TaskName
   :window/type WindowType
   :window/aggregation (s/either s/Keyword [s/Keyword])
   (s/optional-key :window/init) s/Any
   (s/optional-key :window/window-key) s/Any
   (s/optional-key :window/min-key) SPosInt
   (s/optional-key :window/range) Unit
   (s/optional-key :window/slide) Unit
   (s/optional-key :window/timeout-gap) Unit
   (s/optional-key :window/session-key) s/Any
   (s/optional-key :window/doc) s/Str
   UnsupportedWindowKey s/Any})

(def TriggerRefinement
  NamespacedKeyword)

(def TriggerPeriod 
  (s/enum :milliseconds :seconds :minutes :hours :days
          :mullisecond :second :minute :hour :day))

(def TriggerThreshold
  (s/enum :elements :element))

(def UnsupportedTriggerKey
  (s/pred (fn [k]
            (or (not (keyword? k))
                (not (= "trigger" (namespace k)))))
          'unsupported-trigger-key))

(def Trigger
  {:trigger/window-id s/Keyword
   :trigger/refinement TriggerRefinement
   :trigger/on s/Keyword
   :trigger/sync s/Keyword
   (s/optional-key :trigger/fire-all-extents?)
   (deprecated [:trigger-entry :model :trigger/fire-all-extents?])
   (s/optional-key :trigger/changelog?) s/Bool
   (s/optional-key :trigger/pred) s/Keyword
   (s/optional-key :trigger/watermark-percentage) double
   (s/optional-key :trigger/doc) s/Str
   (s/optional-key :trigger/period) [(s/one PosInt "trigger period") 
                                     (s/one TriggerPeriod "threshold type")]
   (s/optional-key :trigger/threshold) [(s/one PosInt "number elements") 
                                        (s/one TriggerThreshold "threshold type")]
   UnsupportedTriggerKey s/Any})

(def StateAggregationCall
  {(s/optional-key :aggregation/init) Function
   :aggregation/create-state-update Function
   :aggregation/apply-state-update Function
   (s/optional-key :aggregation/super-aggregation-fn) Function})

(def RefinementCall
  {:refinement/create-state-update Function
   :refinement/apply-state-update Function})

(def JobScheduler
  NamespacedKeyword)

(def TaskScheduler
  NamespacedKeyword)

(def Job
  {:catalog Catalog
   :workflow Workflow
   :task-scheduler TaskScheduler
   (s/optional-key :percentage) s/Int
   (s/optional-key :flow-conditions) [FlowCondition]
   (s/optional-key :windows) [Window]
   (s/optional-key :triggers) [Trigger]
   (s/optional-key :lifecycles) [Lifecycle]
   (s/optional-key :acker/percentage) s/Int
   (s/optional-key :acker/exempt-input-tasks?) s/Bool
   (s/optional-key :acker/exempt-output-tasks?) s/Bool
   (s/optional-key :acker/exempt-tasks) [s/Keyword]})

(def ClusterId
  (s/either s/Uuid s/Str))

(def EnvConfig
  {:zookeeper/address s/Str
   :onyx/id ClusterId
   (s/optional-key :zookeeper/server?) s/Bool
   (s/optional-key :zookeeper.server/port) s/Int
   (s/optional-key :onyx.bookkeeper/server?) s/Bool
   (s/optional-key :onyx.bookkeeper/delete-server-data?) s/Bool
   (s/optional-key :onyx.bookkeeper/port) s/Int
   (s/optional-key :onyx.bookkeeper/local-quorum?) s/Bool
   (s/optional-key :onyx.bookkeeper/local-quorum-ports) [s/Int]
   (s/optional-key :onyx.bookkeeper/base-journal-dir) s/Str
   (s/optional-key :onyx.bookkeeper/base-ledger-dir) s/Str
   (s/optional-key :onyx.bookkeeper/disk-usage-threshold) (s/pred float?)
   (s/optional-key :onyx.bookkeeper/disk-usage-warn-threshold) (s/pred float?)
   s/Keyword s/Any})

(def AeronIdleStrategy
  (s/enum :busy-spin :low-restart-latency :high-restart-latency))

(def Messaging
  (s/enum :aeron :dummy-messenger))

(def StateLogImpl
  (s/enum :bookkeeper :none))

(def StateFilterImpl
  (s/enum :set :rocksdb))

(def PeerConfig
  {:zookeeper/address s/Str
   :onyx/id ClusterId
   :onyx.peer/job-scheduler JobScheduler
   :onyx.messaging/impl Messaging
   :onyx.messaging/bind-addr s/Str
   (s/optional-key :onyx.messaging/peer-port) s/Int
   (s/optional-key :onyx.messaging/external-addr) s/Str
   (s/optional-key :onyx.peer/inbox-capacity) s/Int
   (s/optional-key :onyx.peer/outbox-capacity) s/Int
   (s/optional-key :onyx.peer/retry-start-interval) s/Int
   (s/optional-key :onyx.peer/join-failure-back-off) s/Int
   (s/optional-key :onyx.peer/drained-back-off) s/Int
   (s/optional-key :onyx.peer/job-not-ready-back-off) s/Int
   (s/optional-key :onyx.peer/peer-not-ready-back-off) s/Int
   (s/optional-key :onyx.peer/fn-params) s/Any
   (s/optional-key :onyx.peer/backpressure-check-interval) s/Int
   (s/optional-key :onyx.peer/backpressure-low-water-pct) s/Int
   (s/optional-key :onyx.peer/backpressure-high-water-pct) s/Int
   (s/optional-key :onyx.peer/state-log-impl) StateLogImpl
   (s/optional-key :onyx.peer/state-filter-impl) StateFilterImpl
   (s/optional-key :onyx.peer/tags) [s/Keyword]
   (s/optional-key :onyx.bookkeeper/client-timeout) PosInt
   (s/optional-key :onyx.bookkeeper/client-throttle) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-password) s/Str
   (s/optional-key :onyx.bookkeeper/ledger-id-written-back-off) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-ensemble-size) PosInt
   (s/optional-key :onyx.bookkeeper/ledger-quorum-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-batch-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-buffer-size) PosInt
   (s/optional-key :onyx.bookkeeper/write-batch-backoff) PosInt
   (s/optional-key :onyx.bookkeeper/read-batch-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/base-dir) s/Str
   (s/optional-key :onyx.rocksdb.filter/bloom-filter-bits) PosInt
   (s/optional-key :onyx.rocksdb.filter/compression) (s/enum :bzip2 :lz4 :lz4hc :none :snappy :zlib)
   (s/optional-key :onyx.rocksdb.filter/block-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/peer-block-cache-size) PosInt
   (s/optional-key :onyx.rocksdb.filter/num-buckets) PosInt
   (s/optional-key :onyx.rocksdb.filter/num-ids-per-bucket) PosInt
   (s/optional-key :onyx.rocksdb.filter/rotation-check-interval-ms) PosInt
   (s/optional-key :onyx.zookeeper/backoff-base-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-sleep-time-ms) s/Int
   (s/optional-key :onyx.zookeeper/backoff-max-retries) s/Int
   (s/optional-key :onyx.zookeeper/prepare-failure-detection-interval) s/Int
   (s/optional-key :onyx.messaging/inbound-buffer-size) s/Int
   (s/optional-key :onyx.messaging/completion-buffer-size) s/Int
   (s/optional-key :onyx.messaging/release-ch-buffer-size) s/Int
   (s/optional-key :onyx.messaging/retry-ch-buffer-size) s/Int
   (s/optional-key :onyx.messaging/peer-link-gc-interval) s/Int
   (s/optional-key :onyx.messaging/peer-link-idle-timeout) s/Int
   (s/optional-key :onyx.messaging/ack-daemon-timeout) s/Int
   (s/optional-key :onyx.messaging/ack-daemon-clear-interval) s/Int
   (s/optional-key :onyx.messaging/decompress-fn) Function
   (s/optional-key :onyx.messaging/compress-fn) Function
   (s/optional-key :onyx.messaging/allow-short-circuit?) s/Bool
   (s/optional-key :onyx.messaging.aeron/embedded-driver?) s/Bool
   (s/optional-key :onyx.messaging.aeron/embedded-media-driver-threading) (s/enum :dedicated :shared :shared-network)
   (s/optional-key :onyx.messaging.aeron/subscriber-count) s/Int
   (s/optional-key :onyx.messaging.aeron/write-buffer-size) s/Int
   (s/optional-key :onyx.messaging.aeron/poll-idle-strategy) AeronIdleStrategy 
   (s/optional-key :onyx.messaging.aeron/offer-idle-strategy) AeronIdleStrategy
   (s/optional-key :onyx.messaging.aeron/publication-creation-timeout) s/Int
   (s/optional-key :onyx.windowing/min-value) s/Int
   (s/optional-key :onyx.task-scheduler.colocated/only-send-local?) s/Bool
   s/Keyword s/Any})

(def PeerId
  (s/either s/Uuid s/Keyword))

(def PeerState
  (s/enum :idle :backpressure :active))

(def PeerSite 
  {s/Any s/Any})

(def JobId
  (s/either s/Uuid s/Keyword))

(def TaskId
  (s/either s/Uuid s/Keyword))

(def TaskScheduler 
  s/Keyword)

(def SlotId
  s/Int)

(def Replica
  {:job-scheduler JobScheduler
   :messaging {:onyx.messaging/impl Messaging s/Keyword s/Any}
   :peers [PeerId]
   :peer-state {PeerId PeerState}
   :peer-sites {PeerId PeerSite}
   :prepared {PeerId PeerId}
   :accepted {PeerId PeerId}
   :pairs {PeerId PeerId}
   :jobs [JobId]
   :task-schedulers {JobId TaskScheduler}
   :tasks {JobId [TaskId]}
   :allocations {JobId {TaskId [PeerId]}}
   :task-metadata {JobId {TaskId s/Any}}
   :saturation {JobId s/Num}
   :task-saturation {JobId {TaskId s/Num}}
   :flux-policies {JobId {TaskId s/Any}}
   :min-required-peers {JobId {TaskId s/Num}}
   :input-tasks {JobId [TaskId]}
   :output-tasks {JobId [TaskId]}
   :exempt-tasks  {JobId [TaskId]}
   :sealed-outputs {JobId #{TaskId}}
   :ackers {JobId [PeerId]} 
   :acker-percentage {JobId s/Int}
   :acker-exclude-inputs {TaskId s/Bool}
   :acker-exclude-outputs {TaskId s/Bool}
   :task-percentages {JobId {TaskId s/Num}}
   :percentages {JobId s/Num}
   :completed-jobs [JobId] 
   :killed-jobs [JobId] 
   :state-logs {JobId {TaskId {SlotId [s/Int]}}}
   :state-logs-marked #{s/Int}
   :task-slot-ids {JobId {TaskId {PeerId SlotId}}}
   :exhausted-inputs {JobId #{TaskId}}
   :required-tags {JobId {TaskId [s/Keyword]}}
   :peer-tags {PeerId [s/Keyword]}})

(def LogEntry
  {:fn s/Keyword
   :args {s/Any s/Any}
   (s/optional-key :message-id) s/Int
   (s/optional-key :created-at) s/Int
   (s/optional-key :peer-parent) s/Uuid
   (s/optional-key :entry-parent) s/Int})

(def Reactions 
  (s/maybe [LogEntry]))

(def ReplicaDiff
  (s/maybe (s/either {s/Any s/Any} #{s/Any})))

(def State
  {s/Any s/Any})
