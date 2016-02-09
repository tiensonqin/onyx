(ns onyx.plugin.simple-input)

(defprotocol SimpleInput
  (start [this])
  (stop [this])
  (recover! [this content])
  (checkpoint-segment! [this value])
  (checkpoint-ack! [this value])
  (complete-segment! [this])
  (drained! [this])
  (checkpoint [this])
  (next-segment! [this next-state]))

(defrecord Next [value checkpoint])
