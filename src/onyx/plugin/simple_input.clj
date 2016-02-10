(ns onyx.plugin.simple-input)

(defprotocol SimpleInput
  (start [this]
         "Initialise the plugin (generally assoc any initial state into your record)")
  (stop [this]
        "Shutdown the input (close any resources that needs to be closed. This can also be done in lifecycles)")
  (checkpoint [this]
              "Pure function that returns a checkpoint offset value that will be periodically written zookeeper. 
              This checkpoint value will be passed in to recover! when restarting the task and recovering the state.")
  (recover! [this checkpoint]
            "Recover the state of the plugin from the supplied checkpoint")
  (checkpoint-ack! [this offset]
                   "Update the checkpoint state as a segment has been acked")
  (segment-complete! [this segment]
                     "Perform any side-effects that you might want to perform as a result of a segment being completed")
  (next-segment! [this]
                 "Return the next segment to be read from the input medium.
                 Must return an (onyx.plugin.simple-input/->SegmentOffset segment checkpoint-offset)"))

(defrecord SegmentOffset [segment offset])
