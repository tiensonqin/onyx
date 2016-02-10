(ns onyx.plugin.simple-input)

(defprotocol SimpleInput
  (start [this]
         "Initialise the plugin (generally assoc any initial state into your record)")
  (stop [this]
        "Shutdown the input (close any resources that needs to be closed. This can also be done in lifecycles)")
  (checkpoint [this]
              "Pure function that returns a checkpoint offset value that will be periodically written zookeeper. 
              This checkpoint value will be passed in to recover when restarting the task and recovering the state.")
  (offset [this]
          "Returns the offset for the current state")
  (segment [this]
           "Returns the segment at the current state")
  (next-state [this]
              "Moves reader to the next state. Returns the reader in the updated state")
  (recover [this checkpoint]
            "Recover the state of the plugin from the supplied checkpoint. Returns a new reader")
  (checkpoint-ack [this offset]
                   "Update the checkpoint state as a segment has been acked. Returns a new reader")
  (segment-complete! [this segment]
                     "Perform any side-effects that you might want to perform as a result of a segment being completed"))

(defrecord SegmentOffset [segment offset])
