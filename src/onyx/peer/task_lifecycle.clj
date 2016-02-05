(ns ^:no-doc onyx.peer.task-lifecycle
    (:require [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
              [onyx.static.rotating-seq :as rsc]
              [onyx.log.commands.common :as common]
              [onyx.log.entry :as entry]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
              [onyx.static.planning :refer [find-task]]
              [onyx.static.uuid :as uuid]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.peer.task-compile :as c]
              [onyx.lifecycles.lifecycle-invoke :as lc]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.peer.function :as function]
              [onyx.peer.operation :as operation]
              [onyx.windowing.window-compile :as wc]
              [onyx.windowing.window-extensions :as we]
              [onyx.windowing.aggregation :as agg]
              [onyx.triggers.triggers-api :as triggers]
              [onyx.triggers.refinements]
              [onyx.extensions :as extensions]
              [onyx.types :refer [->Ack ->Results ->MonitorEvent dec-count! inc-count! map->Event map->Compiled]]
              [onyx.peer.transform :refer [apply-fn]]
              [onyx.peer.grouping :as g]
              [onyx.flow-conditions.fc-routing :as r]
              [onyx.log.commands.peer-replica-view :refer [peer-site]]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn windowed-task? [event]
  (or (not-empty (:onyx.core/windows event))
      (not-empty (:onyx.core/triggers event))))

(defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task (:onyx.core/compiled event) event)]
    (when-not (:onyx.core/start-lifecycle? rets)
      (timbre/info (format "[%s] Peer chose not to start the task yet. Backing off and retrying..."
                           (:onyx.core/id event))))
    rets))

(defn add-acker-id [id m]
  (assoc m :acker-id id))

(defn add-completion-id [id m]
  (assoc m :completion-id id))

(defn sentinel-found? [event]
  (seq (filter #(= :done (:message %))
               (:onyx.core/batch event))))

(defn complete-job [{:keys [onyx.core/job-id onyx.core/task-id] :as event}]
  (let [entry (entry/create-log-entry :exhaust-input {:job job-id :task task-id})]
    (>!! (:onyx.core/outbox-ch event) entry)))

(defn sentinel-id [event]
  (:id (first (filter #(= :done (:message %))
                      (:onyx.core/batch event)))))

(defrecord AccumAckSegments [ack-val segments retries])

(defn add-segments [accum routes hash-group leaf]
  (if (empty? routes)
    accum
    (if-let [route (first routes)]
      (let [ack-val (acker/gen-ack-value)
            grp (get hash-group route)
            leaf* (-> leaf
                      (assoc :ack-val ack-val)
                      (assoc :hash-group grp)
                      (assoc :route route))
            fused-ack (bit-xor ^long (:ack-val accum) ^long ack-val)]
        (-> accum
            (assoc :ack-val fused-ack)
            (update :segments (fn [s] (conj! s leaf*)))
            (add-segments (rest routes) hash-group leaf)))
      (add-segments accum (rest routes) hash-group leaf))))

(defn add-from-leaf [event {:keys [egress-ids task->group-by-fn] :as compiled} result
                     root leaves start-ack-val accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event compiled result message)
        message* (r/flow-conditions-transform message routes event compiled)
        hash-group (g/hash-groups message* egress-ids task->group-by-fn)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (add-segments accum (:flow routes) hash-group leaf*))))

(defn add-from-leaves
  "Flattens root/leaves into an xor'd ack-val, and accumulates new segments and retries"
  [segments retries event result compiled]
  (let [root (:root result)
        leaves (:leaves result)
        start-ack-val (or (:ack-val root) 0)]
    (reduce (fn [accum leaf] 
              (add-from-leaf event compiled result root leaves start-ack-val accum leaf))
            (->AccumAckSegments start-ack-val segments retries)
            leaves)))

(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:acks results))
             (persistent! (:segments results))
             (persistent! (:retries results))))

(defn build-new-segments
  [compiled {:keys [onyx.core/results] :as event}]
  (let [results (reduce (fn [accumulated result]
                          (let [root (:root result)
                                segments (:segments accumulated)
                                retries (:retries accumulated)
                                ret (add-from-leaves segments retries event result compiled)
                                new-ack (->Ack (:id root) (:completion-id root) (:ack-val ret) (atom 1) nil)
                                acks (conj! (:acks accumulated) new-ack)]
                            (->Results (:tree results) acks (:segments ret) (:retries ret))))
                        results
                        (:tree results))]
    (assoc event :onyx.core/results (persistent-results! results))))

(defn ack-segments [{:keys [peer-replica-view task-map state messenger monitoring] :as compiled} 
                    {:keys [onyx.core/results] :as event}]
  (doseq [[acker-id acks] (->> (:acks results)
                               (filter dec-count!)
                               (group-by :completion-id))]
    (when-let [site (peer-site peer-replica-view acker-id)]
      (emit-latency :peer-ack-segments
                    monitoring
                    #(extensions/internal-ack-segments messenger event site acks))))
  event)

(defn flow-retry-segments [{:keys [peer-replica-view state messenger monitoring] :as compiled} {:keys [onyx.core/results] :as event}]
  (doseq [root (:retries results)]
    (when-let [site (peer-site peer-replica-view (:completion-id root))]
      (emit-latency :peer-retry-segment
                    monitoring
                    #(extensions/internal-retry-segment messenger event (:id root) site))))
  event)

(defn gen-lifecycle-id [event]
  (assoc event :onyx.core/lifecycle-id (uuid/random-uuid)))

(defn handle-backoff! [event]
  (let [batch (:onyx.core/batch event)]
    (when (and (= (count batch) 1)
               (= (:message (first batch)) :done))
      (Thread/sleep (:onyx.core/drained-back-off event)))))

(defn read-batch [{:keys [peer-replica-view task-type pipeline job-id] :as compiled} event]
  (if (and (= task-type :input) (:backpressure? @peer-replica-view))
    (assoc event :onyx.core/batch '())
    (let [rets (merge event (p-ext/read-batch pipeline event))
          rets (merge event (lc/invoke-after-read-batch compiled rets))]
      (handle-backoff! event)
      rets)))

(defn tag-messages [{:keys [peer-replica-view task-type id] :as compiled} event]
  (if (= task-type :input)
    (update event
            :onyx.core/batch
            (fn [batch]
              (map (fn [segment]
                     (add-acker-id ((:pick-acker-fn @peer-replica-view))
                                   (add-completion-id id segment)))
                   batch)))
    event))

(defn add-messages-to-timeout-pool [{:keys [task-type state]} event]
  (when (= task-type :input)
    (swap! state update :timeout-pool rsc/add-to-head
           (map :id (:onyx.core/batch event))))
  event)

(defn process-sentinel [{:keys [task-type monitoring pipeline]} event]
  (if (and (= task-type :input)
           (sentinel-found? event))
    (do
      (extensions/emit monitoring (->MonitorEvent :peer-sentinel-found))
      (if (p-ext/drained? pipeline event)
        (complete-job event)
        (p-ext/retry-segment pipeline event (sentinel-id event)))
      (update event
              :onyx.core/batch
              (fn [batch]
                (remove (fn [v] (= :done (:message v)))
                        batch))))
    event))

(defn replay-windows-from-log
  [{:keys [onyx.core/window-state onyx.core/state-log] :as event}]
  (when (windowed-task? event)
    (swap! window-state 
           (fn [wstate] 
             (let [compiled-apply-fn (wc/compile-apply-window-entry-fn event)
                   replayed-state (state-extensions/playback-log-entries state-log event wstate compiled-apply-fn)]
               (trace (:onyx.core/task-id event) "replayed state:" replayed-state)
               replayed-state))))
  event)

(defn window-state-updates [segment widstate w grouping-fn]
  (let [window-id (:window/id w)
        record (:aggregate/record w)
        segment-coerced (we/uniform-units record segment)
        widstate' (we/speculate-update record widstate segment-coerced)
        widstate'' (if grouping-fn 
                     (merge-with #(we/merge-extents record % (:aggregate/super-agg-fn w) segment-coerced) widstate')
                     (we/merge-extents record widstate' (:aggregate/super-agg-fn w) segment-coerced))
        extents (we/extents record (keys widstate'') segment-coerced)
        grp-key (if grouping-fn (grouping-fn segment))]
    (let [record (:aggregate/record w)]
      (reduce (fn [[wst entries] extent]
                (let [extent-state (get wst extent)
                      state-value (-> (if grouping-fn (get extent-state grp-key) extent-state)
                                      (agg/default-state-value w))
                      state-transition-entry ((:aggregate/fn w) state-value w segment)
                      new-state-value ((:aggregate/apply-state-update w) state-value state-transition-entry)
                      new-state (if grouping-fn
                                  (assoc extent-state grp-key new-state-value)
                                  new-state-value)
                      log-value (if grouping-fn 
                                  (list extent state-transition-entry grp-key)
                                  (list extent state-transition-entry))]
                  (list (assoc wst extent new-state)
                        (conj entries log-value))))
              (list widstate'' [])
              extents)))) 

(defn triggers-state-updates [event triggers notification state log-entries changelogs]
  (reduce
    (fn [[state entries cgs] {:keys [trigger/window-id trigger/id] :as t}]
      (if (and (some #{(:context notification)} (triggers/trigger-notifications event t)) 
               (triggers/trigger-fire? event t notification))
        (let [t-changelog (changelogs id)
              [new-window-id-state entry] (triggers/fire-trigger! event (get state window-id) t notification t-changelog)
              window-state (assoc state window-id new-window-id-state)
              updated-changelog (dissoc cgs id)]
          (list window-state (conj entries entry) updated-changelog))
        (list state (conj entries nil) cgs)))
    (list state log-entries changelogs)
    triggers))

;; Fix to not require grouping-fn in window-state-updates
(defn windows-state-updates [segment grouping-fn windows state-log-entries]
  (reduce (fn [[state log-entries] {:keys [window/id] :as window}]
            (let [window-id-state (get state id)
                  [window-id-state' window-entries] (window-state-updates segment window-id-state window grouping-fn)
                  window-state (assoc state id window-id-state')]
              (list window-state (conj log-entries window-entries))))
          state-log-entries
          windows))

(defn add-to-changelogs 
  "Adds to a map of trigger-ids -> changelogs, where a changelog is a vector of 
  state updates that have occurred since the last time the trigger fired for a window"
  [changelog windows triggers log-entry]
  (let [window-id->changes (zipmap (map :window/id windows) (rest log-entry))]
    (reduce (fn [m {:keys [trigger/id trigger/window-id]}]
              (update m id (fn [changes] 
                             (into (or changes []) 
                                   (window-id->changes window-id)))))
            changelog
            (filter :trigger/changelog? triggers))))

(defn assign-windows
  [{:keys [peer-replica-view] :as compiled} {:keys [onyx.core/windows] :as event}]
  (when (seq windows)
    (let [{:keys [onyx.core/monitoring onyx.core/state onyx.core/messenger 
                  onyx.core/triggers onyx.core/windows onyx.core/task-map onyx.core/window-state 
                  onyx.core/state-log onyx.core/results]} event
          grouping-fn (:grouping-fn compiled)
          uniqueness-check? (contains? task-map :onyx/uniqueness-key)
          id-key (:onyx/uniqueness-key task-map)] 
      (doall
        (map 
          (fn [leaf fused-ack]
            (let [start-time (System/currentTimeMillis)
                  ;; Message should only be acked when all log updates have been written
                  ;; As we filter out messages seen before, some replay can be accepted
                  ack-fn (fn [] 
                           (when (dec-count! fused-ack)
                             (when-let [site (peer-site peer-replica-view (:completion-id fused-ack))]
                               (extensions/internal-ack-segment messenger event site fused-ack)))
                           (emit-latency-value :window-log-write-entry monitoring (- (System/currentTimeMillis) start-time)))]
              (run! 
                (fn [message]
                  (let [segment (:message message)
                        unique-id (if uniqueness-check? (get segment id-key))
                        filter? (and unique-id (state-extensions/filter? (:filter @window-state) event unique-id))]
                    (when-not filter?
                      (inc-count! fused-ack)
                      (let [initial-entry [unique-id]
                            [new-window-state log-entry] (windows-state-updates segment grouping-fn windows (list (:state @window-state) initial-entry))
                            changes (add-to-changelogs (:changelogs @window-state) windows triggers log-entry)
                            notification {:segment segment :context :new-segment}
                            [new-window-state log-entry changes*] (triggers-state-updates event triggers notification new-window-state log-entry changes)] 
                        (swap! window-state assoc :state new-window-state :changelogs changes*)
                        (state-extensions/store-log-entry state-log event ack-fn log-entry)))
                    ;; Always update the filter, to freshen up the fact that the id has been re-seen
                    (when uniqueness-check? 
                      (swap! window-state update :filter state-extensions/apply-filter-id event unique-id))))
                (:leaves leaf))))
          (:tree results)
          (:acks results)))))
  event)

(defn write-batch [{:keys [pipeline]} event]
  (let [rets (merge event (p-ext/write-batch pipeline event))]
    (taoensso.timbre/trace (format "[%s / %s] Wrote %s segments"
                                   (:onyx.core/id rets)
                                   (:onyx.core/lifecycle-id rets)
                                   (count (:onyx.core/results rets))))
    rets))

(defn launch-aux-threads!
  [messenger {:keys [onyx.core/pipeline
                     onyx.core/compiled
                     onyx.core/messenger-buffer
                     onyx.core/monitoring
                     onyx.core/replica
                     onyx.core/peer-replica-view
                     onyx.core/state] :as event}
   outbox-ch seal-ch completion-ch task-kill-ch]
  (thread
   (try
     (let [{:keys [retry-ch release-ch]} messenger-buffer]
       (loop []
         (when-let [[v ch] (alts!! [task-kill-ch completion-ch seal-ch release-ch retry-ch])]
           (when v
             (cond (= ch release-ch)
                   (->> (p-ext/ack-segment pipeline event v)
                        (lc/invoke-after-ack event compiled v))

                   (= ch completion-ch)
                   (let [{:keys [id peer-id]} v
                         site (peer-site peer-replica-view peer-id)]
                     (when site 
                       (emit-latency :peer-complete-segment
                                     monitoring
                                     #(extensions/internal-complete-segment messenger event id site))))

                   (= ch retry-ch)
                   (->> (p-ext/retry-segment pipeline event v)
                        (lc/invoke-after-retry event compiled v))

                   (= ch seal-ch)
                   (do
                     (p-ext/seal-resource pipeline event)
                     (let [entry (entry/create-log-entry :seal-output {:job (:onyx.core/job-id event)
                                                                       :task (:onyx.core/task-id event)})]
                       (>!! outbox-ch entry))))
             (recur)))))
     (catch Throwable e
       (fatal e)))))

(defn input-retry-segments! [messenger {:keys [onyx.core/pipeline
                                               onyx.core/compiled]
                                        :as event}
                             input-retry-timeout task-kill-ch]
  (go
    (when (= :input (:onyx/type (:onyx.core/task-map event)))
      (loop []
        (let [timeout-ch (timeout input-retry-timeout)
              ch (second (alts!! [timeout-ch task-kill-ch]))]
          (when (= ch timeout-ch)
            (let [tail (last (get-in @(:onyx.core/state event) [:timeout-pool]))]
              (doseq [m tail]
                (when (p-ext/pending? pipeline event m)
                  (taoensso.timbre/trace (format "Input retry message %s" m))
                  (->> (p-ext/retry-segment pipeline event m)
                       (lc/invoke-after-retry event compiled m))))
              (swap! (:onyx.core/state event) update :timeout-pool rsc/expire-bucket)
              (recur))))))))

(defn setup-triggers [event]
  (reduce triggers/trigger-setup
          event
          (:onyx.core/triggers event)))

(defn teardown-triggers [event]
  (reduce triggers/trigger-teardown
          event
          (:onyx.core/triggers event)))

(defn handle-exception [log e restart-ch outbox-ch job-id]
  (let [data (ex-data e)]
    (if (:onyx.core/lifecycle-restart? data)
      (warn (:original-exception data) "Caught exception inside task lifecycle. Rebooting the task.")
      (do (warn e "Handling uncaught exception thrown inside task lifecycle - killing this job.")
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            (extensions/write-chunk log :exception e job-id)
            (>!! outbox-ch entry))))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [{:keys [onyx.core/compiled] :as init-event} seal-ch kill-ch ex-f]
  (try
    (while (first (alts!! [seal-ch kill-ch] :default true))
      (->> init-event
           (gen-lifecycle-id)
           (lc/invoke-before-batch compiled)
           (lc/invoke-read-batch read-batch compiled)
           (tag-messages compiled)
           (add-messages-to-timeout-pool compiled)
           (process-sentinel compiled)
           (apply-fn compiled)
           (build-new-segments compiled)
           (lc/invoke-assign-windows assign-windows compiled)
           (lc/invoke-write-batch write-batch compiled)
           (flow-retry-segments compiled)
           (lc/invoke-after-batch compiled)
           (ack-segments compiled)))
    (catch Throwable e
      (ex-f e))))

(defn validate-pending-timeout [pending-timeout opts]
  (when (> pending-timeout (arg-or-default :onyx.messaging/ack-daemon-timeout opts))
    (throw (ex-info "Pending timeout cannot be greater than acking daemon timeout"
                    {:opts opts :pending-timeout pending-timeout}))))


(defn build-pipeline [task-map pipeline-data]
  (let [kw (:onyx/plugin task-map)]
    (try
      (if (#{:input :output} (:onyx/type task-map))
        (case (:onyx/language task-map)
          :java (operation/instantiate-plugin-instance (name kw) pipeline-data)
          (let [user-ns (namespace kw)
                user-fn (name kw)
                pipeline (if (and user-ns user-fn)
                           (if-let [f (ns-resolve (symbol user-ns) (symbol user-fn))]
                             (f pipeline-data)))]
            (or pipeline
                (throw (ex-info "Failure to resolve plugin builder fn.
                                 Did you require the file that contains this symbol?" {:kw kw})))))
        (onyx.peer.function/function pipeline-data))
      (catch Throwable e
        (throw (ex-info "Failed to resolve or build plugin on the classpath, did you require/import the file that contains this plugin?" {:symbol kw :exception e}))))))

(defn exactly-once-task? [event]
  (boolean (get-in event [:onyx.core/task-map :onyx/uniqueness-key])))

(defn resolve-log [{:keys [onyx.core/peer-opts] :as pipeline}]
  (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
    (assoc pipeline :onyx.core/state-log (if (windowed-task? pipeline) 
                                           (state-extensions/initialize-log log-impl pipeline)))))

(defrecord TaskState [timeout-pool])

(defrecord WindowState [filter state changelogs ack-state])

(defn resolve-window-state [{:keys [onyx.core/peer-opts] :as pipeline}]
  (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
    (assoc pipeline :onyx.core/window-state (if (windowed-task? pipeline)
                                              (atom (->WindowState (if (exactly-once-task? pipeline) 
                                                                     (state-extensions/initialize-filter filter-impl pipeline)) 
                                                                   {}
                                                                   {}
                                                                   {}))))))

(defrecord TaskInformation 
  [id log job-id task-id 
   workflow catalog task flow-conditions windows filtered-windows triggers lifecycles task-map]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          filtered-windows (wc/filter-windows windows (:name task))
          workflow (extensions/read-chunk log :workflow job-id)
          triggers (extensions/read-chunk log :triggers job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          task-map (find-task catalog (:name task))]
      (assoc component 
             :workflow workflow :catalog catalog :task task :flow-conditions flow-conditions :windows windows 
             :filtered-windows filtered-windows :triggers triggers :lifecycles lifecycles :task-map task-map)))
  (stop [component]
    (assoc component 
           :catalog nil :task nil :flow-conditions nil :windows nil 
           :filtered-windows nil :triggers nil :lifecycles nil :task-map nil)))

(defn new-task-information [peer-state task-state]
  (map->TaskInformation (select-keys (merge peer-state task-state) [:id :log :job-id :task-id])))

(defrecord TaskLifeCycle
  [id log messenger-buffer messenger job-id task-id replica peer-replica-view restart-ch
   kill-ch outbox-ch seal-ch completion-ch opts task-kill-ch task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (try
      (let [{:keys [workflow catalog task flow-conditions windows filtered-windows triggers lifecycles task-map]} task-information
            ;; Number of buckets in the timeout pool is covered over a 60 second
            ;; interval, moving each bucket back 60 seconds / N buckets
            input-retry-timeout (arg-or-default :onyx/input-retry-timeout task-map)
            pending-timeout (arg-or-default :onyx/pending-timeout task-map)
            r-seq (rsc/create-r-seq pending-timeout input-retry-timeout)
            state (atom (->TaskState r-seq))

            _ (taoensso.timbre/info (format "[%s] Warming up Task LifeCycle for job %s, task %s" id job-id (:name task)))
            _ (validate-pending-timeout pending-timeout opts)

            pipeline-data {:onyx.core/id id
                           :onyx.core/job-id job-id
                           :onyx.core/task-id task-id
                           :onyx.core/task (:name task)
                           :onyx.core/catalog catalog
                           :onyx.core/workflow workflow 
                           :onyx.core/flow-conditions flow-conditions
                           :onyx.core/lifecycles lifecycles
                           :onyx.core/compiled (map->Compiled {})
                           :onyx.core/task-map task-map
                           :onyx.core/serialized-task task
                           :onyx.core/drained-back-off (arg-or-default :onyx.peer/drained-back-off opts)
                           :onyx.core/log log
                           :onyx.core/messenger-buffer messenger-buffer
                           :onyx.core/messenger messenger
                           :onyx.core/monitoring task-monitoring
                           :onyx.core/outbox-ch outbox-ch
                           :onyx.core/seal-ch seal-ch
                           :onyx.core/restart-ch restart-ch
                           :onyx.core/task-kill-ch task-kill-ch
                           :onyx.core/kill-ch kill-ch
                           :onyx.core/peer-opts opts
                           :onyx.core/fn (operation/resolve-task-fn task-map)
                           :onyx.core/replica replica
                           :onyx.core/peer-replica-view peer-replica-view
                           :onyx.core/state state}

            add-pipeline (fn [event]
                           (assoc event 
                                  :onyx.core/pipeline 
                                  (build-pipeline task-map event)))

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               (c/windows->event-map filtered-windows)
                               add-pipeline
                               c/task->event-map)

            ex-f (fn [e] (handle-exception log e restart-ch outbox-ch job-id))
            _ (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                          (not (start-lifecycle? pipeline-data)))
                (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts)))

            pipeline-data (->> pipeline-data
                              (lc/invoke-before-task-start (:onyx.core/compiled pipeline-data))
                              resolve-window-state
                              resolve-log
                              replay-windows-from-log
                              (c/resolve-window-triggers triggers filtered-windows)
                              setup-triggers)]

        (>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))

        (loop [replica-state @replica]
          (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
                     (or (not (common/job-covered? replica-state job-id))
                         (not (common/any-ackers? replica-state job-id))))
            (taoensso.timbre/info (format "[%s] Not enough virtual peers have warmed up to start the task yet, backing off and trying again..." id))
            (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
            (recur @replica)))

        (taoensso.timbre/info (format "[%s] Enough peers are active, starting the task" id))

        (let [input-retry-segments-ch (input-retry-segments! messenger pipeline-data input-retry-timeout task-kill-ch)
              aux-ch (launch-aux-threads! messenger pipeline-data outbox-ch seal-ch completion-ch task-kill-ch)
              task-lifecycle-ch (thread (run-task-lifecycle pipeline-data seal-ch kill-ch ex-f))]
          (assoc component
                 :pipeline-data pipeline-data
                 :seal-ch seal-ch
                 :task-kill-ch task-kill-ch
                 :task-lifecycle-ch task-lifecycle-ch
                 :input-retry-segments-ch input-retry-segments-ch
                 :aux-ch aux-ch)))
      (catch Throwable e
        (handle-exception log e restart-ch outbox-ch job-id)
        component)))

  (stop [component]
    (if-let [task-name (:onyx.core/task (:pipeline-data component))]
      (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle for %s" id task-name))
      (taoensso.timbre/info (format "[%s] Stopping Task LifeCycle, failed to initialize task set up." id)))
    (when-let [event (:pipeline-data component)]

      ;; At some point trigger state updates for :task-lifecycle-stopped should be stored via store-log-entry here
      #_(when-let [triggers (seq (:onyx.core/triggers event))]
        (triggers-state-updates event 
                                triggers
                                {:context :task-lifecycle-stopped}
                                (list (:state @(:onyx.core/window-state event)) 
                                      (repeat (inc (count (:onyx.core/windows event))) nil))))

        ;; Ensure task operations are finished before closing peer connections
        (close! (:seal-ch component))
        (<!! (:task-lifecycle-ch component))
        (close! (:task-kill-ch component))

        (<!! (:input-retry-segments-ch component))
        (<!! (:aux-ch component))

        (teardown-triggers event)

        (when-let [state-log (:onyx.core/state-log event)] 
          (state-extensions/close-log state-log event))

        (when-let [window-state (:onyx.core/window-state event)] 
        (when (exactly-once-task? event)
          (state-extensions/close-filter (:filter @window-state) event)))

      ((:compiled-after-task-fn (:onyx.core/compiled event)) event))

    (assoc component
      :pipeline-data nil
      :seal-ch nil
      :aux-ch nil
      :input-retry-segments-ch nil
      :task-lifecycle-ch nil)))

(defn task-lifecycle [peer-state task-state]
  (map->TaskLifeCycle (merge peer-state task-state)))
