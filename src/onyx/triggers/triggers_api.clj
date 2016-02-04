(ns onyx.triggers.triggers-api
  (:require [onyx.static.planning :refer [find-window]]
            [onyx.windowing.units :refer [to-standard-units coerce-key]]
            [onyx.windowing.window-id :as wid]
            [onyx.windowing.window-extensions :as we]
            [onyx.static.default-vals :as d]
            [taoensso.timbre :refer [info warn fatal]]))

;   @window-state)

; {:trigger/setup
;  :trigger/notifications
;  :trigger/fire?
;  :trigger/teardown
;  :trigger/refinement
;  :trigger/refine-state 
;  :trigger/notifications
;  :trigger/fire? 
;  :trigger/refinement}


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

(defmulti refine-state
  "Updates the local window state according to the refinement policy.
   Must return the new local window state in its entirety."
  (fn [event trigger]
    (:trigger/refinement trigger)))

(defmulti refinement-destructive?
  "Returns true if this refinement mode destructs local state."
  (fn [event trigger]
    (:trigger/refinement trigger)))

(defmethod refine-state :accumulating
  [{:keys [onyx.core/window-state]} trigger]
  @window-state)

(def refine-discarding 
  {:refinement/state-update (fn [event trigger state])
   :refinement/apply-state-update (fn [event trigger state entry]
                                    {})})

(def refine-accumulating
  {:refinement/state-update (fn [event trigger state])
   :refinement/apply-state-update (fn [event trigger state entry]
                                    state)})

(def refinements {:discarding refine-discarding
                  :accumulating refine-accumulating})

(defmethod refinement-destructive? :discarding
  [event trigger]
  true)

(defmethod refinement-destructive? :default
  [event trigger]
  false)

(defmethod trigger-setup :default
  [event trigger]
  event)

(defmethod trigger-teardown :default
  [event trigger]
  event)

   (fn [[window-id-state* entries] [window-id state]]
     (let [window (find-window (:onyx.core/windows event) (:trigger/window-id trigger))
           [lower upper] (we/bounds (:aggregate/record window) window-id)
           args (merge opts
                       {:window window :window-id window-id
                        :lower-extent lower :upper-extent upper})]
       (if (f event trigger args)
         (let [window-metadata {:window-id window-id
                                :lower-bound lower
                                :upper-bound upper
                                :context (:context opts)}
               {:keys [:refinement/state-update :refinement/apply-state-update]} refine-accumulating ;refine-discarding
               refinement-entry (state-update event trigger)]
           ((:trigger/sync-fn trigger) event window trigger window-metadata state)
           (list (apply-state-update event window-id-state* refinement-entry)
                 (conj entries refinement-entry)))
         entries)))
   (list window-id-state [])
   window-id-state))

(defn fire-trigger! [event window-id-state trigger opts]
  (if (some #{(:context opts)} (trigger-notifications event trigger))
    (if (:trigger/fire-all-extents? trigger)
      (when (trigger-fire? event trigger opts)
        (iterate-windows event trigger window-id-state (constantly true) opts))
      (iterate-windows event trigger window-id-state trigger-fire? opts)))
  (list window-id-state []))
