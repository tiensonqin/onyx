(ns onyx.triggers.refinements)

(def discarding 
  {:refinement/create-state-update (fn discarding-create-state-update 
                                     [trigger opts state])
   :refinement/apply-state-update (fn discarding-apply-state-update 
                                    [trigger opts state entry]
                                    {})})

(def accumulating
  {:refinement/create-state-update (fn accumulating-create-state-update 
                                     [trigger opts state])
   :refinement/apply-state-update (fn accumulating-apply-state-update
                                    [trigger opts state entry]
                                    state)})
