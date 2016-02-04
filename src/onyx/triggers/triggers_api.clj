(ns onyx.triggers.triggers-api
  (:require [onyx.static.planning :refer [find-window]]
            [onyx.windowing.units :refer [to-standard-units coerce-key]]
            [onyx.windowing.window-id :as wid]
            [onyx.windowing.window-extensions :as we]
            [onyx.static.default-vals :as d]
            [taoensso.timbre :refer [info warn fatal]]))

(defmulti trigger-setup
  "Sets up any vars or state to subsequently
   use in trigger invocations. Must return an
   updated event map."
  (fn [event trigger]
    (:trigger/on trigger)))

(defmulti trigger-notifications
  "Returns a set of keywords denoting notifications that
   this trigger responds to. Currently only supports `:new-segment`."
  (fn [event trigger]
    (:trigger/on trigger)))

(defmulti trigger-fire?
  "Returns true if this trigger should fire, therefore refinining the
   state of each extent in this window and invoking the trigger sync function.
   This function is invoked exactly once per window, so this function may
   perform side-effects such as maintaining counters."
  (fn [event trigger & args]
    (:trigger/on trigger)))

(defmulti trigger-teardown
  "Tears down any vars or state created to support this trigger.
   Must return an updated event map."
  (fn [event trigger]
    (:trigger/on trigger)))

(defmethod trigger-setup :default
  [event trigger]
  event)

(defmethod trigger-teardown :default
  [event trigger]
  event)

(defn fire-trigger! 
  [{:keys [onyx.core/windows] :as event} 
   window-id-state 
   {:keys [trigger/window-id trigger/sync-fn refinement/state-update refinement/apply-state-update] :as trigger}
   notification 
   changelog]
  (let [window (find-window windows window-id)
        extents-bounds (map (partial we/bounds (:aggregate/record window)) (keys window-id-state))
        entry (state-update event trigger window-id-state changelog)
        opts (merge notification {:extents-bounds extents-bounds
                                  :refinement-entry entry
                                  :changelog changelog})
        new-state (apply-state-update event trigger window-id-state entry)]
    (sync-fn event window trigger opts window-id-state new-state)
    (list new-state entry)))
