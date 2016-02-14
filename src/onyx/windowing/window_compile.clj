(ns ^:no-doc onyx.windowing.window-compile
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.static.validation :as validation]
            [onyx.windowing.window-extensions :as w]
            [onyx.windowing.aggregation :as a]
            [onyx.state.state-extensions :as st]
            [onyx.schema :refer [InternalWindow InternalTrigger Trigger Window Event]]
            [onyx.types :refer [map->InternalTrigger]]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [only]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.grouping :as g]
            [schema.core :as s]))

(defn filter-windows [windows task]
  (filter #(= (:window/task %) task) windows))

(defn compacted-reset? [entry]
  (and (map? entry)
       (= (:type entry) :compacted)))

(defn unpack-compacted [state {:keys [filter-snapshot extent-state]} event]
  (-> state
      (assoc :state extent-state)
      (update :filter st/restore-filter event filter-snapshot)))

(defn resolve-window-init [window calls]
  (if-not (:aggregation/init calls)
    (let [init (:window/init window)]
      (when-not init
        (throw (ex-info "No :window/init supplied, this is required for this aggregation" {:window window})))
      (constantly (:window/init window)))
    (:aggregation/init calls)))

(defn filter-ns-key-map [m ns-str]
  (->> m
       (filter (fn [[k _]] (= ns-str (namespace k))))
       (map (fn [[k v]] [(keyword (name k)) v]))
       (into {})))

(s/defn resolve-window :- InternalWindow
  [window :- Window]
  (let [agg (:window/aggregation window)
        agg-var (if (sequential? agg) (first agg) agg)
        calls (var-get (kw->fn agg-var))
        _ (validation/validate-state-aggregation-calls calls)]
    (-> window
        (filter-ns-key-map "window")
        (assoc :window window
               :init-fn (resolve-window-init window calls)
               :create-state-update (:aggregation/create-state-update calls)
               :super-agg-fn (:aggregation/super-aggregation-fn calls)
               :apply-state-update (:aggregation/apply-state-update calls))
        ((w/windowing-builder window)))))

(s/defn find-window :- InternalWindow 
  [windows :- [InternalWindow] window-id]
  (let [matches (filter #(= window-id (:id %)) windows)]
    (only matches)))

(s/defn resolve-trigger :- InternalTrigger
  [windows :- [InternalWindow]
   {:keys [trigger/sync trigger/refinement trigger/window-id] :as trigger} :- Trigger]
  (let [refinement-calls (var-get (kw->fn refinement))] 
    (validation/validate-refinement-calls refinement-calls)
    (let [trigger (assoc trigger :trigger/id (random-uuid))] 
      (-> trigger
          (filter-ns-key-map "trigger")
          (update :changelog? (fn [ch] (if (nil? ch) true ch)))  
          (assoc :internal-window (find-window windows (:trigger/window-id trigger)))
          (assoc :trigger trigger)
          (assoc :sync-fn (kw->fn sync))
          (assoc :refinement-calls refinement-calls)    
          map->InternalTrigger))))

(defn compile-apply-window-entry-fn [{:keys [onyx.core/task-map onyx.core/windows] :as event}]
  (let [grouped-task? (g/grouped-task? task-map)
        get-state-fn (if grouped-task? 
                       (fn [ext-state grp-key] 
                         (get ext-state grp-key)) 
                       (fn [ext-state grp-key] 
                         ext-state))
        set-state-fn (if grouped-task?
                       (fn [ext-state grp-key new-value] 
                         (assoc ext-state grp-key new-value))
                       (fn [ext-state grp-key new-value] 
                         new-value))
        apply-window-entries 
        (fn [state [window-entries {:keys [id apply-state-update] :as window}]]
          (reduce (fn [state* [extent extent-entry grp-key]]
                    (if (nil? extent-entry)
                      ;; Destructive triggers turn the state to nil,
                      ;; prune these out of the window state to avoid
                      ;; inflating memory consumption.
                      (update-in state* [:state id] dissoc extent)
                      (update-in state* 
                                 [:state id extent]
                                 (fn [ext-state] 
                                   (let [state-value (a/default-state-value window (get-state-fn ext-state grp-key))
                                         new-state-value (apply-state-update window state-value extent-entry)] 
                                     (set-state-fn ext-state grp-key new-state-value))))))
                  state
                  window-entries))
        extents-fn (fn [state log-entry] 
                     (reduce apply-window-entries 
                             state 
                             (map list (rest log-entry) windows)))]
    (fn [state entry]
      (if (compacted-reset? entry)
        (unpack-compacted state entry event)
        (let [unique-id (first entry)
              _ (trace "Playing back entries for segment with id:" unique-id)
              new-state (extents-fn state entry)]
          (if unique-id
            (update new-state :filter st/apply-filter-id event unique-id)
            new-state))))))
