---
layout: user_guide_page
title: Peer Configuration
categories: [user-guide-page]
---

## Peer Configuration

The chapter describes the all options available to configure the virtual peers and development environment.

### Base Configuration

| key name                      | type       |
|-------------------------------|------------|
|`:onyx/id`                     |  `any`     |
|`:zookeeper/address`           |  `string`  |


### Environment Only

| key name                            | type       | optional?  |
|-------------------------------------|------------|------------|
|`:zookeeper/server?`                 | `boolean`  | Yes        |
|`:zookeeper.server/port`             | `int`      | Yes        |
|`:onyx.bookkeeper/server?`           | `boolean`  | Yes        |
|`:onyx.bookkeeper/local-quorum?`     | `boolean`  | Yes        |
|`:onyx.bookkeeper/local-quorum-ports`| `[int]`    | Yes        |
|`:onyx.bookkeeper/port`              | `int`      | Yes        |
|`:onyx.bookkeeper/base-journal-dir`  | `string`   | Yes        |
|`:onyx.bookkeeper/base-ledger-dir`   | `string`   | Yes        |

### Peer Only

#### Base Configuration

| key name                                   | type       | default                            |
|--------------------------------------------|------------|------------------------------------|
|`:onyx.peer/inbox-capacity`                 | `int`      | `1000`                             |
|`:onyx.peer/outbox-capacity`                | `int`      | `1000`                             |
|`:onyx.peer/retry-start-interval`           | `int`      | `2000`                             |
|`:onyx.peer/join-failure-back-off`          | `int`      | `250`                              |
|`:onyx.peer/drained-back-off`               | `int`      | `400`                              |
|`:onyx.peer/peer-not-ready-back-off`        | `int`      | `2000`                             |
|`:onyx.peer/job-not-ready-back-off`         | `int`      | `500`                              |
|`:onyx.peer/fn-params`                      | `map`      | `{}`                               |
|`:onyx.peer/tags`                           | `vector`   | `[]`                               |
|`:onyx.peer/backpressure-check-interval`    | `int`      | `10`                               |
|`:onyx.peer/backpressure-low-water-pct`     | `int`      | `30`                               |
|`:onyx.peer/backpressure-high-water-pct`    | `int`      | `60`                               |
|`:onyx.zookeeper/backoff-base-sleep-time-ms`| `int`      | `1000`                             |
|`:onyx.zookeeper/backoff-max-sleep-time-ms` | `int`      | `30000`                            |
|`:onyx.zookeeper/backoff-max-retries`       | `int`      | `5`                                |
|`:onyx.messaging/inbound-buffer-size`       | `int`      | `20000`                            |
|`:onyx.messaging/completion-buffer-size`    | `int`      | `1000`                             |
|`:onyx.messaging/release-ch-buffer-size`    | `int`      | `10000`                            |
|`:onyx.messaging/retry-ch-buffer-size`      | `int`      | `10000`                            |
|`:onyx.messaging/peer-link-gc-interval`     | `int`      | `90000`                            |
|`:onyx.messaging/peer-link-idle-timeout`    | `int`      | `60000`                            |
|`:onyx.messaging/ack-daemon-timeout`        | `int`      | `60000`                            |
|`:onyx.messaging/ack-daemon-clear-interval` | `int`      | `15000`                            |
|`:onyx.messaging/decompress-fn`             | `function` | `onyx.compression.nippy/decompress`|
|`:onyx.messaging/compress-fn`               | `function` | `onyx.compression.nippy/compress`  |
|`:onyx.messaging/impl`                      | `keyword`  | `:aeron`                           |
|`:onyx.messaging/bind-addr`                 | `string`   | `nil`                              |
|`:onyx.messaging/peer-port`                 | `int`      | `nil`                              |
|`:onyx.messaging/allow-short-circuit?`      | `boolean`  | `true`                             |
|`:onyx.messaging.aeron/embedded-driver?`    | `boolean`  | `true`                             |
|`:onyx.messaging.aeron/subscriber-count`    | `int`      | `2`                                |
|`:onyx.messaging.aeron/write-buffer-size`   | `int`      | `1000`                             |
|`:onyx.messaging.aeron/poll-idle-strategy`  | `keyword`  | `:high-restart-latency`            |
|`:onyx.messaging.aeron/offer-idle-strategy` | `keyword`  | `:high-restart-latency`            |

##### `:onyx.peer/inbox-capacity`

Maximum number of messages to try to prefetch and store in the inbox, since reading from the log happens asynchronously.

##### `:onyx.peer/outbox-capacity`

Maximum number of messages to buffer in the outbox for writing, since writing to the log happens asynchronously.

##### `:onyx.peer/retry-start-interval`

Number of ms to wait before trying to reboot a virtual peer after failure.

##### `:onyx.peer/drained-back-off`

Number of ms to wait before trying to complete the job if all input tasks have been exhausted. Completing the job may not succeed if the cluster configuration is being shifted around.

##### `:onyx:onyx.peer/peer-not-ready-back-off`

Number of ms to back off and wait before retrying the call to `start-task?` lifecycle hook if it returns false.

##### `:onyx:onyx.peer/job-not-ready-back-off`

Number of ms to back off and wait before trying to discover configuration needed to start the subscription after discovery failure.

##### `:onyx.peer/join-failure-back-off`

Number of ms to wait before trying to rejoin the cluster after a previous join attempt has aborted.

##### `:onyx.peer/fn-params`

A map of keywords to vectors. Keywords represent task names, vectors represent the first parameters to apply
to the function represented by the task. For example, `{:add [42]}` for task `:add` will call the function
underlying `:add` with `(f 42 <segment>)`.

##### `:onyx.peer/tags`

Tags which denote the capabilities of this peer in terms of user-defined functionality. Must be specified as a vector of keywords. This is used in combination with `:onyx/required-tags` in the catalog to force tasks to run on certain sets of machines.

##### `:onyx.zookeeper/backoff-base-sleep-time-ms`

Initial amount of time to wait between ZooKeeper connection retries

##### `:onyx.zookeeper/backoff-max-sleep-time-ms`

Maximum amount of time in ms to sleep on each retry

##### `:onyx.zookeeper/backoff-max-retries`

Maximum number of times to retry connecting to ZooKeeper

##### `:onyx.peer/backpressure-low-water-pct`

Percentage of messaging inbound-buffer-size that constitutes a low water mark for backpressure purposes.

##### `:onyx.peer/backpressure-high-water-pct`

Percentage of messaging inbound-buffer-size that constitutes a high water mark for backpressure purposes.

##### `:onyx.peer/backpressure-check-interval`

Number of ms between checking whether the virtual peer should notify the cluster of backpressure-on/backpressure-off.

##### `:onyx.messaging/inbound-buffer-size`

Number of messages to buffer in the core.async channel for received segments.

##### `:onyx.messaging/completion-buffer-size`

Number of messages to buffer in the core.async channel for completing messages on an input task.

##### `:onyx.messaging/release-ch-buffer-size`

Number of messages to buffer in the core.async channel for released completed messages.

##### `:onyx.messaging/retry-ch-buffer-size`

Number of messages to buffer in the core.async channel for retrying timed-out messages.

##### `:onyx.messaging/peer-link-gc-interval`

The interval in milliseconds to wait between closing idle peer links.

##### `:onyx.messaging/peer-link-idle-timeout`

The maximum amount of time that a peer link can be idle (not looked up in the state atom for usage) before it is eligible to be closed. The connection will be reopened from scratch the next time it is needed.

##### `:onyx.messaging/ack-daemon-timeout`

Number of milliseconds that an ack value can go without being updates on a daemon before it is eligible to time out.

##### `:onyx.messaging/ack-daemon-clear-interval`

Number of milliseconds to wait for process to periodically clear out ack-vals that have timed out in the daemon.

##### `:onyx.messaging/decompress-fn`

The Clojure function to use for messaging decompression. Receives one argument - a byte array. Must return the decompressed value of the byte array.

##### `:onyx.messaging/compress-fn`

The Clojure function to use for messaging compression. Receives one argument - a sequence of segments. Must return a byte array representing the segment seq.

##### `:onyx.messaging/impl`

The messaging protocol to use for peer-to-peer communication.

##### `:onyx.messaging/bind-addr`

An IP address to bind the peer to for messaging. Defaults to `nil`, binds to it's external IP to the result of calling `http://checkip.amazonaws.com`.

##### `:onyx.messaging/peer-port`

The port that peers should use to communicate.

##### `:onyx.messaging/allow-short-circuit?`

A boolean denoting whether to allow virtual peers to short circuit networked messaging when colocated with the other virtual peer. Short circuiting allows for direct transfer of messages to a virtual peer's internal buffers, which improves performance where possible. This configuration option is primarily for use in perfomance testing, as peers will not generally be able to short circuit messaging after scaling to many nodes.

##### `:onyx.messaging.aeron/embedded-driver?`

A boolean denoting whether an Aeron media driver should be started up with the environment. See [Aeron Media Driver](https://github.com/onyx-platform/onyx/blob/0.8.8/src/onyx/messaging/aeron_media_driver.clj) for an example for how to start the media driver externally.

##### `:onyx.messaging.aeron/subscriber-count`

The number of Aeron subscriber threads that receive messages for the peer-group.  As peer-groups are generally configured per-node (machine), this setting can bottleneck receive performance if many virtual peers are used per-node, or are receiving and/or de-serializing large volumes of data. A good guidline is is `num cores = num virtual peers + num subscribers`, assuming virtual peers are generally being fully utilised.

##### `:onyx.messaging.aeron/write-buffer-size`

Size of the write queue for the Aeron publication. Writes to this queue will currently block once full.

##### `:onyx.messaging.aeron/poll-idle-strategy`

The Aeron idle strategy to use between when polling for new messages. Currently, two choices `:high-restart-latency` and `:low-restart-latency` can be chosen. low-restart-latency may result in lower latency message, at the cost of higher CPU usage or potentially reduced throughput.

##### `:onyx.messaging.aeron/offer-idle-strategy`

The Aeron idle strategy to use between when offering messages to another peer. Currently, two choices `:high-restart-latency` and `:low-restart-latency` can be chosen. low-restart-latency may result in lower latency message, at the cost of higher CPU usage or potentially reduced throughput.

### Peer Full Example

```clojure
(def peer-opts
  {:onyx/id "df146eb8-fd6e-4903-847e-9e748ca08021"
   :zookeeper/address "127.0.0.1:2181"
   :onyx.peer/inbox-capacity 2000
   :onyx.peer/outbox-capacity 2000
   :onyx.peer/retry-start-interval 4000
   :onyx.peer/join-failure-back-off 500
   :onyx.peer/drained-back-off 400
   :onyx.peer/peer-not-ready-back-off 5000
   :onyx.peer/job-not-ready-back-off 1000
   :onyx.peer/fn-params {:add [42]}
   :onyx.peer/zookeeper-timeout 10000
   :onyx.messaging/completion-buffer-size 2000
   :onyx.messaging/release-ch-buffer-size 50000
   :onyx.messaging/retry-ch-buffer-size 100000
   :onyx.messaging/ack-daemon-timeout 90000
   :onyx.messaging/ack-daemon-clear-interval 15000
   :onyx.messaging/decompress-fn onyx.compression.nippy/decompress
   :onyx.messaging/compress-fn onyx.compression.nippy/compress
   :onyx.messaging/impl :aeron
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/peer-port-range [50000 60000]
   :onyx.messaging/peer-ports [45000 45002 42008]})
```
