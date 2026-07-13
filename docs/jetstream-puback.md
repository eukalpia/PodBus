# JetStream PubAck operation guide

PodBus waits for a JetStream publish acknowledgement before `enqueue` completes.
The acknowledgement proves that the target JetStream server accepted the
message into the stream according to the configured storage and replication
policy. It does not prove that a worker processed the message.

## Concurrent confirmation routing

`dart_nats 1.1.1` serializes its generic request/reply path through one global
mutex. Using that path for every JetStream publish collapses concurrent
publication into one request queue.

PodBus instead creates one wildcard reply subscription per JetStream adapter.
Every publish receives a unique child reply subject. PubAck responses are routed
by that subject and may complete out of order.

The inbox prefix is generated with the NATS NUID implementation backed by
`Random.secure()`. The service account must still be protected by broker ACLs:
allow the service to use its required `_INBOX` reply subjects and do not grant
unrelated publishers broad access to `_INBOX.>`.

## Publish lifecycle

For each publish PodBus:

1. creates a unique reply subject below the adapter inbox;
2. records a pending confirmation with an absolute deadline;
3. publishes the message with `Nats-Msg-Id` when an idempotency key is present;
4. waits for the matching PubAck;
5. validates protocol status, server error, stream name, sequence, and duplicate metadata;
6. removes the pending entry on success, error, or timeout.

A late PubAck after timeout is ignored because its pending entry no longer
exists. It cannot satisfy another publish.

## Timeout ambiguity

A PubAck timeout does not prove that the message was rejected. The server may
have persisted the message while the acknowledgement was lost. Retrying can
therefore deliver a duplicate.

Use a stable message ID or idempotency key for retries where possible. Consumers
must still make externally visible side effects idempotent because duplicate
delivery can also occur after worker crashes or acknowledgement loss.

## Shutdown and drain

`drain()` stops accepting new publishes and waits for the confirmations already
in flight. Each wait is bounded by the original publish deadline; shutdown does
not create a new unbounded wait.

If a pending acknowledgement reaches its deadline during drain, PodBus:

- fails drain with the original timeout;
- fails remaining pending confirmations;
- closes the reply inbox and NATS client;
- leaves no unresolved confirmation futures.

`close()` is immediate: outstanding confirmations fail because the adapter is
closing. Use drain when confirmed delivery is part of graceful shutdown and
close when the process must abandon outstanding work.

Set the platform termination grace period above the largest publish timeout plus
application cleanup time.

## Capacity

The concurrent inbox removes the library-wide request mutex, but it is not an
invitation to create unbounded application futures. Bound publication with a
worker pool, queue, or semaphore sized from measured PubAck latency and stream
capacity.

Monitor:

- PubAck latency percentiles;
- confirmation timeout and server-error count;
- application in-flight publish count;
- NATS connection reconnects and pending bytes;
- stream storage, replicas, and cluster health;
- durable consumer pending, ack-pending, and redelivery counts;
- pull fetch latency and empty-fetch rate.

Throughput is limited by storage mode, replication, disk, network, payload size,
server flow control, and consumer acknowledgements. The beta qualification
numbers are regression evidence from one environment, not a service-level
promise.

## Security boundary

Treat the NATS credential as a service identity, not a shared cluster password.
Grant only:

- publish permission for the service subjects it owns;
- subscribe permission for the subjects it consumes;
- the JetStream API and reply permissions required by the deployment;
- access to its own reply inbox pattern.

TLS authenticates the broker endpoint. Subject permissions authorize actions
after authentication. A random inbox is defense in depth, not a replacement for
permissions.

## Regression requirements

Changes to the PubAck path must preserve:

- concurrent out-of-order acknowledgements;
- isolation of one failed publish from other confirmations;
- timeout cleanup and late-reply rejection;
- immediate failure on close;
- deadline-bounded drain;
- reconnect and inbox-replacement cleanup;
- message-ID propagation and duplicate metadata;
- the 250,000-message durable and worker qualification profiles.
