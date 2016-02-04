(ns onyx.windowing.trigger-punctuation-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.triggers.triggers-api :as api]
            [onyx.api]))

(def true-pred (constantly true))

(def false-pred (constantly false))

(deftest punctuation-true-pred
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :onyx.triggers.refinements/accumulating
                 :trigger/on :punctuation
                 :trigger/pred ::true-pred
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/punctuation-preds {:trigger-id true-pred}}]
    (is
     (api/trigger-fire? event trigger {:window window :segment segment}))))

(deftest punctuation-false-pred
  (let [window {:window/id :collect-segments
                :window/task :identity
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/count
                :window/window-key :event-time
                :window/range [5 :minutes]}
        trigger {:trigger/window-id :collect-segments
                 :trigger/refinement :onyx.triggers.refinements/accumulating
                 :trigger/on :punctuation
                 :trigger/pred ::false-pred
                 :trigger/sync ::no-op
                 :trigger/id :trigger-id}
        segment {}
        event {:onyx.triggers/punctuation-preds {:trigger-id false-pred}}]
    (is
     (not (api/trigger-fire? event trigger {:window window :segment segment})))))
