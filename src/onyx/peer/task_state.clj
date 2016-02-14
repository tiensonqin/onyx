(ns ^:no-doc onyx.peer.task-state
    (:require [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
              [onyx.schema :refer [InternalTrigger InternalWindow Window Event]]
              [schema.core :as s]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
              [onyx.static.uuid :as uuid]
              [onyx.windowing.window-extensions :as we]
              [onyx.windowing.aggregation :as agg]
              [onyx.triggers.triggers-api :as triggers :refer [fire-trigger! trigger-fire? trigger-notifications]]
              [onyx.triggers.refinements]
              [onyx.extensions :as extensions]
              [onyx.types :refer [->Ack ->Results ->MonitorEvent dec-count! inc-count! map->Event map->Compiled]]
              [onyx.state.ack :as st-ack]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [defaults arg-or-default]]))


;;; 1. Increment ack counts
;;; 2. Put batch of results on channel
;;; 3. Have thread read from channel, do each message in turn
;;; 4. 


;; Windowing record with both the internal window, and all the internal triggers it needs
;; Plus the internal state, and the last changelog entry

;; Takes a message and returns the next state, record has the log entry in it
;; In order to checkpoint, just do reductions over the messages to get all the intermediate states
;; and intermediate log entries

;; Transactionally batch {:ids XXXX :windows {window1 [entries] :window2 [entries, including-trigger-entries - write out the trigger for each one initially]}


(s/defn window-state-updates 
  [segment 
   widstate 
   {:keys [window super-agg-fn create-state-update apply-state-update] :as iw} :- InternalWindow 
   grouping-fn]
  (let [segment-coerced (we/uniform-units iw segment)
        widstate' (we/speculate-update iw widstate segment-coerced)
        widstate'' (if grouping-fn 
                     (merge-with #(we/merge-extents iw % super-agg-fn segment-coerced) widstate')
                     (we/merge-extents iw widstate' super-agg-fn segment-coerced))
        extents (we/extents iw (keys widstate'') segment-coerced)
        grp-key (if grouping-fn (grouping-fn segment))]
    (reduce (fn [[wst entries] extent]
              (let [extent-state (get wst extent)
                    state-value (->> (if grouping-fn (get extent-state grp-key) extent-state)
                                     (agg/default-state-value iw))
                    state-transition-entry (create-state-update window state-value segment)
                    new-state-value (apply-state-update window state-value state-transition-entry)
                    new-state (if grouping-fn
                                (assoc extent-state grp-key new-state-value)
                                new-state-value)
                    log-value (if grouping-fn 
                                (list extent state-transition-entry grp-key)
                                (list extent state-transition-entry))]
                (list (assoc wst extent new-state)
                      (conj entries log-value))))
            (list widstate'' [])
            extents))) 

(s/defn triggers-state-updates 
  [event :- Event triggers :- [InternalTrigger] notification state log-entries changelogs]
  (reduce
    (s/fn [[state entries cgs] 
           {:keys [window-id id trigger] :as t} :- InternalTrigger]
      (if (and (some #{(:context notification)} 
                     (trigger-notifications event trigger)) 
               (trigger-fire? event trigger notification))
        (let [t-changelog (changelogs id)
              wid-state (get state window-id) 
              notification (assoc notification :aggregation/changelog t-changelog)
              [new-wid-state entry] (fire-trigger! event wid-state t notification)
              window-state (assoc state window-id new-wid-state)
              updated-changelog (dissoc cgs id)]
          (list window-state (conj entries entry) updated-changelog))
        (list state (conj entries nil) cgs)))
    (list state log-entries changelogs)
    triggers))

;; Fix to not require grouping-fn in window-state-updates
(s/defn windows-state-updates 
  [segment grouping-fn windows :- [InternalWindow] state-log-entries]
  (reduce (fn [[state log-entries] {:keys [id] :as window}]
            (let [wid-state (get state id)
                  [new-wid-state win-entries] (window-state-updates segment wid-state window grouping-fn)
                  window-state (assoc state id new-wid-state)]
              (list window-state (conj log-entries win-entries))))
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
            (filter :changelog? triggers))))

(s/defn assign-windows :- Event
  [{:keys [peer-replica-view] :as compiled} 
   {:keys [onyx.core/windows] :as event} :- Event]
  (when (seq windows)
    (let [{:keys [onyx.core/monitoring onyx.core/state onyx.core/messenger 
                  onyx.core/triggers onyx.core/windows onyx.core/task-map onyx.core/window-state 
                  onyx.core/acking-state onyx.core/state-log onyx.core/results]} event
          grouping-fn (:grouping-fn compiled)
          acker (:acking-state compiled)
          uniqueness-check? (contains? task-map :onyx/uniqueness-key)
          id-key (:onyx/uniqueness-key task-map)] 
      (doall
        (map 
          (fn [leaf fused-ack]
              (run! 
                (fn [message]
                  (let [segment (:message message)
                        unique-id (if uniqueness-check? (get segment id-key))]
                    ;; don't use unique-id as the id may be nil
                    (if-not (and uniqueness-check? (state-extensions/filter? (:filter @window-state) event unique-id))
                      (let [_ (st-ack/prepare acker unique-id fused-ack)
                            initial-entry [unique-id]
                            [new-win-state log-entry] (windows-state-updates segment grouping-fn windows (list (:state @window-state) initial-entry))
                            changes (add-to-changelogs (:changelogs @window-state) windows triggers log-entry)
                            notification {:context :new-segment :segment segment}
                            [new-win-state log-entry changes*] (triggers-state-updates event triggers notification new-win-state log-entry changes)
                            start-time (System/currentTimeMillis)
                            success-fn (fn [] 
                                         (st-ack/ack acker unique-id fused-ack)
                                         (emit-latency-value :window-log-write-entry monitoring (- (System/currentTimeMillis) start-time)))] 
                        (swap! window-state assoc :state new-win-state :changelogs changes*)
                        (state-extensions/store-log-entry state-log event success-fn log-entry))
                      (st-ack/defer acker unique-id fused-ack))
                    ;; Always update the filter, to freshen up the fact that the id has been re-seen
                    (when uniqueness-check? 
                      (swap! window-state update :filter state-extensions/apply-filter-id event unique-id))))
                (:leaves leaf)))
          (:tree results)
          (:acks results)))))
  event)
