(ns onyx.triggers.refinements
  (:require [onyx.schema :refer [Trigger]]
            [schema.core :as s]))

(def Opts {s/Any s/Any})

(def discarding 
  {:refinement/create-state-update (s/fn discarding-create-state-update 
                                     [trigger :- Trigger opts :- Opts state])
   :refinement/apply-state-update (s/fn discarding-apply-state-update 
                                    [trigger :- Trigger opts :- Opts state entry]
                                    {})})

(def accumulating
  {:refinement/create-state-update (s/fn accumulating-create-state-update 
                                     [trigger :- Trigger opts :- Opts state])
   :refinement/apply-state-update (s/fn accumulating-apply-state-update
                                    [trigger :- Trigger opts :- Opts state entry]
                                    state)})
