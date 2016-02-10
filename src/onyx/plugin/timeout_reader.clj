(ns onyx.plugin.timeout-reader
  (:require [clojure.core.async :refer [chan >! >!! <!! close! thread timeout alts!! go-loop sliding-buffer]]
            [onyx.plugin.simple-input :refer [checkpoint-ack! checkpoint next-segment! recover! segment-complete! stop]]
            [onyx.static.swap-pair :refer [swap-pair!]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn start-commit-loop! [reader shutdown-ch commit-ms log k]
  (go-loop []
           (let [timeout-ch (timeout commit-ms)] 
             (when-let [[_ ch] (alts!! [shutdown-ch timeout-ch] :priority true)]
               (when (= ch timeout-ch)
                 (extensions/force-write-chunk log :chunk (checkpoint reader) k)
                 (recur))))))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defn completed? [batch pending-messages]
  (and (all-done? (vals @pending-messages))
       (all-done? batch)
       (or (not (empty? @pending-messages))
           (not (empty? batch)))))

(defrecord TimeoutInput [reader log task-id max-pending batch-size batch-timeout pending-messages drained? retry-buffer]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          ;; Timeout does not currently work
          timeout-ch (timeout batch-timeout)
          [old-buffer new-buffer] (swap-pair! retry-buffer (fn [v] (drop max-segments v)))
          retry-batch (transient (vec (take max-segments old-buffer)))
          batch (if (= max-segments (count retry-batch))
                  (persistent! retry-batch)
                  (loop [batch retry-batch 
                         {:keys [segment offset]} (next-segment! reader)]
                    (let [new-input (assoc (t/input (random-uuid) segment) :checkpoint offset)
                          new-batch (conj! batch new-input)]
                      (if-not (and (= :done segment) (< (count batch) max-segments))
                        (recur new-batch (next-segment! reader))
                        (persistent! new-batch)))))]
      (when (empty? batch)
        (Thread/sleep 500))
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      (when (completed? batch pending-messages) 
        (extensions/force-write-chunk log :chunk :complete task-id)
        (reset! drained? true))
      {:onyx.core/batch batch #_(take 2 (shuffle batch))}))

  p-ext/PipelineInput
  (ack-segment [_ event segment-id]
    (let [input (get @pending-messages segment-id)] 
      (checkpoint-ack! reader (:checkpoint input))
      (segment-complete! reader (:message input))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! retry-buffer conj (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn new-timeout-input [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event}]
  (let [max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        reader-builder (kw->fn (:simple-input/build-input task-map))
        reader (onyx.plugin.simple-input/start (reader-builder event))]
    (->TimeoutInput reader log task-id max-pending batch-size batch-timeout (atom {}) (atom false) (atom []))))

(defn inject-timeout-reader
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} 
   lifecycle]
  (let [shutdown-ch (chan 1)
        {:keys [reader read-ch]} pipeline
        ;; Attempt to write initial checkpoint
        _ (extensions/write-chunk log :chunk (checkpoint reader) task-id)
        read-offset (extensions/read-chunk log :chunk task-id)]
    (if (= :complete read-offset)
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [commit-ms 500
            _ (recover! reader read-offset)
            commit-loop-ch (start-commit-loop! reader shutdown-ch commit-ms log task-id)]
        {:timeout-reader/reader reader
         :timeout-reader/shutdown-ch shutdown-ch}))))

(defn close-timeout-reader
  [{:keys [timeout-reader/reader timeout-reader/shutdown-ch] :as event} 
   lifecycle]
  (close! shutdown-ch)
  (stop reader)
  {})
