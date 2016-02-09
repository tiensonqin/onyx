(ns onyx.plugin.simple-input)

(defprotocol SimpleInput
  (start [this]
         "Initialise the plugin (generally assoc any initial state into your record)")
  (stop [this]
        "Shutdown the input (close any resources that needs to be closed. This can also be done in lifecycles)")
  (checkpoint [this]
              "Pure function that returns a checkpoint value that will be periodically written zookeeper.")
  (recover! [this checkpoint]
            "Recover the state of the plugin from the supplied checkpoint")
  (checkpoint-segment! [this offset]
                       "Update the checkpoint state, as a segment has been read")
  (checkpoint-ack! [this offset]
                   "Update the checkpoint state as a segment has been acked")
  (segment-complete! [this segment]
                     "Perform any side-effects that you might want to perform as a result of a segment being completed")
  (next-segment! [this prev-offset]
                 "Return the next segment to be read from the input medium.
                 next-state is the checkpoint information of the previous message read e.g. a kafka offset, an array index, etc
                 Must return an (onyx.plugin.simple-input/->Next segment next-offset)"))

(defrecord Next [value offset])
