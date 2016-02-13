(ns onyx.triggers.timer
  (:require [onyx.windowing.units :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.schema :refer [InternalTrigger Trigger Window Event]]
            [schema.core :as s]
            [taoensso.timbre :refer [fatal]]))

(s/defmethod api/trigger-setup :timer
  [{:keys [onyx.core/window-state onyx.core/state-log] :as event} :- Event
   {:keys [trigger/period trigger/id trigger/window-id] :as trigger} :- Trigger]
  (if (= (standard-units-for (second period)) :milliseconds)
    (let [ms (apply to-standard-units period)
          fut
          (future
            (loop []
              (try
                (Thread/sleep ms)
                (let [state (get (:state @window-state) window-id)
                      changelog (get (:changelog @window-state) id)
                      [new-state entries] (api/fire-trigger! event state trigger {:context :timer} changelog)]
                  (swap! window-state assoc :state new-state)
                  (swap! window-state update :changelog dissoc id)
                  ;; TODO: impossible to do this correctly as designed - should disable refinements for triggered
                  ;; for now until solved
                  ;; Should also disallow other triggers in combination with it
                  #_(when (and state (api/refinement-destructive? event trigger))
                      ;; Write the trigger to the log.
                      (let [entry [nil [nil (map (fn [[k v]] [k nil]) state)]]]
                        (state-extensions/store-log-entry state-log event (constantly true) entry))))
                (catch InterruptedException e
                  (throw e))
                (catch Throwable e
                  (fatal e)))
              (recur)))]
      (assoc-in event [:onyx.triggers/period-threads id] fut))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" {:trigger trigger}))))

(defmethod api/trigger-notifications :timer
  [event trigger]
  #{:timer :task-lifecycle-stopped})

(defmethod api/trigger-fire? :timer
  [event trigger args]
  true)

(defmethod api/trigger-teardown :timer
  [event {:keys [trigger/id] :as trigger}]
  (let [fut (get-in event [:onyx.triggers/period-threads id])]
    (future-cancel fut)
    (update-in event [:onyx.triggers/period-threads] dissoc id)))
