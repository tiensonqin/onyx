(ns onyx.triggers.refinements)

(def discarding 
  {:refinement/state-update (fn [event trigger opts state])
   :refinement/apply-state-update (fn [event trigger state entry]
                                    {})})

(def accumulating
  {:refinement/state-update (fn [event trigger opts state])
   :refinement/apply-state-update (fn [event trigger state entry]
                                    state)})
