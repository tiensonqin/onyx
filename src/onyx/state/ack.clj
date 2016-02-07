(ns ^:no-doc onyx.state.ack
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.extensions :as extensions]
            [onyx.types :refer [dec-count! inc-count!]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]))

;; Rename to something like DependentAck, or Trigger ack?
(defprotocol AckState 
  (prepare [this id ack-val])
  (defer [this id ack-val])
  (ack [this id ack-val]))

;; Adapted from Prismatic Plumbing:
;; https://github.com/Prismatic/plumbing/blob/c53ba5d0adf92ec1e25c9ab3b545434f47bc4156/src/plumbing/core.cljx#L346-L361
(defn swap-pair!
  "Like swap! but returns a pair [old-val new-val]"
  ([a f]
     (loop []
       (let [old-val @a
             new-val (f old-val)]
         (if (compare-and-set! a old-val new-val)
           [old-val new-val]
           (recur)))))
  ([a f & args]
     (swap-pair! a #(apply f % args))))

(defrecord StandardAcker [peer-replica-view messenger]
  AckState
  (prepare [this _ ack-val]
    (inc-count! ack-val)
    this)

  (defer [this _ ack-val]
    this)

  (ack [this _ ack-val]
    (when (dec-count! ack-val)
      (when-let [site (peer-site peer-replica-view (:completion-id ack-val))]
        (extensions/internal-ack-segment messenger site ack-val)))
    this))

(defrecord DeduplicationAckState [ack-state peer-replica-view messenger]
  AckState
  (prepare [this id ack-val]
    (let [[old-val new-val] (swap-pair! ack-state
                                        (fn [s] 
                                          (if (s id)
                                            s
                                            (assoc s id (list ack-val)))))]
      (when-not (= old-val new-val)
        (inc-count! ack-val)))
    this)

  (defer [this id ack-val]
    (let [[old-val new-val] (swap-pair! ack-state 
                                        (fn [s]
                                          (if (s id)
                                            (update s id conj ack-val)
                                            s)))]
      (when-not (= old-val new-val)
        (inc-count! ack-val)) 
      this))

  (ack [this id _]
    (let [[old-val new-val] (swap-pair! ack-state (fn [s] (dissoc s id)))]
      (when-not (= old-val new-val)
        (run! (fn [ack-val] 
                (when (dec-count! ack-val)
                  (when-let [site (peer-site peer-replica-view (:completion-id ack-val))]
                    (extensions/internal-ack-segment messenger site ack-val)))) 
              (old-val id))) 
      this)))

(defn new-ack-state [task-map peer-replica-view messenger]
  (if (contains? task-map :onyx/uniqueness-key)
    (->DeduplicationAckState (atom {}) peer-replica-view messenger)
    (->StandardAcker peer-replica-view messenger)))


