(ns onyx.windowing.aggregation-count-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]))

(def input
  [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :age 3  :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def expected-windows
  [[#inst "2015-09-13T03:00:00.000-00:00" #inst "2015-09-13T03:04:59.999-00:00" 3]
   [#inst "2015-09-13T03:05:00.000-00:00" #inst "2015-09-13T03:09:59.999-00:00" 5]
   [#inst "2015-09-13T03:15:00.000-00:00" #inst "2015-09-13T03:19:59.999-00:00" 2]
   [#inst "2015-09-13T03:25:00.000-00:00" #inst "2015-09-13T03:29:59.999-00:00" 1]
   [#inst "2015-09-13T03:45:00.000-00:00" #inst "2015-09-13T03:49:59.999-00:00" 1]
   [#inst "2015-09-13T03:55:00.000-00:00" #inst "2015-09-13T03:59:59.999-00:00" 2]
   [#inst "2015-09-13T03:30:00.000-00:00" #inst "2015-09-13T03:34:59.999-00:00" 1]])

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [window extent->bounds changelog] :as opts} state]
  (doall 
    (map (fn [[extent extent-state] [lower-bound upper-bound]] 
           (swap! test-state conj [(java.util.Date. lower-bound)
                                   (java.util.Date. upper-bound)
                                   extent-state]))
         state
         (map extent->bounds (keys state)))))

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest count-test
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
        batch-size 20
        workflow
        [[:in :identity] [:identity :out]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :identity
          :onyx/fn :clojure.core/identity
          :onyx/type :function
          :onyx/max-peers 1
          :onyx/uniqueness-key :id
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/plugin :onyx.plugin.core-async/output
          :onyx/type :output
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Writes segments to a core.async channel"}]

        windows
        [{:window/id :collect-segments
          :window/task :identity
          :window/type :fixed
          :window/aggregation :onyx.windowing.aggregation/count
          :window/window-key :event-time
          :window/range [5 :minutes]}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/refinement :onyx.triggers.refinements/accumulating
          :trigger/on :segment
          :trigger/threshold [15 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}
         {:lifecycle/task :in
          :lifecycle/calls :onyx.plugin.core-async/reader-calls}
         {:lifecycle/task :out
          :lifecycle/calls ::out-calls}
         {:lifecycle/task :out
          :lifecycle/calls :onyx.plugin.core-async/writer-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! out-chan (chan (sliding-buffer (inc (count input)))))
    (reset! test-state [])

    (with-test-env [test-env [3 env-config peer-config]]
      (onyx.api/submit-job
       peer-config
       {:catalog catalog
        :workflow workflow
        :lifecycles lifecycles
        :windows windows
        :triggers triggers
        :task-scheduler :onyx.task-scheduler/balanced})
      
      (doseq [i input]
        (>!! @in-chan i))
      (>!! @in-chan :done)

      (close! @in-chan)

      (let [results (take-segments! @out-chan)]
        (is (= (into #{} input) (into #{} (butlast results))))
        (is (= :done (last results)))
        (is (= expected-windows @test-state))))))
