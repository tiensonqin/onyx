(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordination.")

;; Replica interface

(defmulti apply-log-entry (fn [entry replica] (:fn entry)))

(defmulti replica-diff (fn [entry old new] (:fn entry)))

(defmulti fire-side-effects! (fn [entry old new diff local-state] (:fn entry)))

(defmulti reactions (fn [entry old new diff peer-args] (:fn entry)))

;; Peer replica view interface

(defmulti peer-replica-view 
  (fn [log entry old-replica new-replica diff old-view state opts] 
    :default))

;; Log interface

(defmulti write-log-entry (fn [log data] (type log)))

(defmulti read-log-entry (fn [log position] (type log)))

(defmulti register-pulse (fn [log id] (type log)))

(defmulti peer-exists? (fn [log id] (type log)))

(defmulti on-delete (fn [log id ch] (type log)))

(defmulti subscribe-to-log (fn [log ch] (type log)))

(defmulti write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti force-write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti read-chunk (fn [log kw id & args] [(type log) kw]))

(defmulti update-origin! (fn [log replica message-id] (type log)))

(defmulti gc-log-entry (fn [log position] (type log)))

;; Messaging interface

(defmulti assign-acker-resources (fn [replica peer-id peer-site peer-sites]
                                  (:onyx.messaging/impl (:messaging replica))))

(defmulti assign-task-resources (fn [replica peer-id task-id peer-site peer-sites]
                                  (:onyx.messaging/impl (:messaging replica))))

(defmulti peer-site (fn [messenger] (type messenger)))

(defmulti get-peer-site (fn [replica peer]
                          (:onyx.messaging/impl (:messaging replica))))

(defmulti register-acker (fn [messenger assigned] (type messenger)))

(defmulti register-task-peer (fn [messenger assigned buffers] (type messenger)))

(defmulti unregister-task-peer (fn [messenger assigned] (type messenger)))

(defmulti connection-spec (fn [messenger peer-id event peer-site] (type messenger)))

(defmulti receive-messages (fn [messenger event] (type messenger)))

(defmulti send-messages (fn [messenger peer-link messages] (type messenger)))

(defmulti close-peer-connection (fn [messenger event link] (type messenger)))

(defmulti internal-ack-segment (fn [messenger peer-link ack]
                                  (type messenger)))

(defmulti internal-ack-segments (fn [messenger peer-link acks]
                                  (type messenger)))

(defmulti internal-complete-segment (fn [messenger id peer-link] (type messenger)))

(defmulti internal-retry-segment (fn [messenger id peer-link] (type messenger)))

;; Monitoring interface

(defmulti monitoring-agent :monitoring)

(defprotocol IEmitEvent
  (registered? [_ event-type])
  (emit [_ event]))
