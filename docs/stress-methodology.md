# Stress and qualification methodology

PodBus stress results are regression and capacity evidence for a documented environment. They are not a universal broker leaderboard.

## Required metadata

Every retained stress artifact should identify:

- PodBus commit SHA;
- Dart SDK version;
- operating system and kernel;
- CPU count and memory;
- broker image/version;
- persistence mode;
- acknowledgement/confirmation mode;
- message count and payload size;
- publisher task count;
- consumer connection count and handler concurrency;
- elapsed wall-clock time;
- unique deliveries, duplicates, and missing messages;
- final broker/container resource snapshot.

A throughput number without those inputs is decorative, not reproducible.

## NATS Core profile

The mandatory Core profile publishes 1,000,000 messages through a separate publisher connection. Consumers use independent NATS connections joined to the same queue group.

This matters because multiple logical subscriptions on one socket still share one client read loop and one operating-system send buffer. Saturating that socket measures a single client connection's slow-consumer threshold, not the queue group's ability to distribute work.

Before the measured burst begins, every subscriber performs a health-check flush. That creates a server round trip after subscription registration, replacing arbitrary readiness sleeps with an explicit barrier.

The runner tracks all message indices in a fixed-size byte bitmap:

- `0` means not observed;
- `1` means observed;
- a second observation increments the duplicate counter.

The bitmap uses approximately one byte per expected message and avoids storing one million Dart objects.

NATS Core is at-most-once. The mandatory test therefore requires all expected messages to arrive in the controlled local environment, but that result must not be reinterpreted as a durability guarantee.

## JetStream profiles

JetStream durable and worker profiles start the durable consumer before enqueueing. The measured interval includes publication acknowledgement and confirmed end-to-end handler delivery.

Relevant settings include:

- stream storage mode;
- `ackWait`;
- `maxDeliver`;
- `maxAckPending`;
- fetch batch size;
- worker concurrency;
- handler delay;
- publisher task count.

A benchmark that fills the stream first and starts the worker later answers a backlog-drain question. It does not measure the steady end-to-end path. PodBus keeps those workloads conceptually separate.

## RabbitMQ profiles

RabbitMQ uses publisher confirms and mandatory routing. The timer includes broker confirmation and, for worker scenarios, successful handler delivery and acknowledgement.

PodBus publisher lanes provide bounded parallelism:

- each AMQP publisher channel has at most one unconfirmed publish;
- calls are distributed round-robin across independent channels;
- a nack or mandatory return fails the matching publish;
- a failed lane operation does not poison its queue;
- reconnect recreates all channels and exchange declarations.

The lane count, broker storage mode, queue durability, message persistence, prefetch, and consumer concurrency must be reported with the result.

## Experimental Kafka profiles

Kafka profiles remain non-blocking evidence until rebalance and delivery-report semantics are promoted from experimental status. Their failure must be visible, but it must not be folded into NATS or RabbitMQ maturity claims.

## Failure criteria

A required profile fails when any of the following occurs:

- the requested scenario is skipped;
- the harness exits non-zero;
- an expected message is missing;
- a broker or client reports a fatal connection error;
- the scenario exceeds its bounded timeout;
- a durable publish is reported successful without its required broker acknowledgement;
- retained evidence or machine metadata is missing.

## Interpreting throughput

Do not compare these as if they were equivalent operations:

- NATS Core fire-and-forget publication;
- JetStream publication with `PubAck`;
- RabbitMQ persistent publication with publisher confirm;
- Kafka delivery reports;
- durable worker throughput including handler acknowledgement.

The useful comparisons are:

1. the same profile across PodBus commits;
2. the same profile before and after a configuration change;
3. the measured workload against its production capacity target;
4. latency and resource behavior during injected faults.

## Reproduction

The large matrix is defined in `.github/workflows/stress.yml`. Individual scenarios can be reproduced by exporting the same environment variables and running either:

```bash
dart run tool/stress_nats_core.dart
```

or:

```bash
dart run tool/stress_transports.dart
```

Use clean broker state for every independent profile. Reusing queues, streams, anonymous volumes, or retained messages invalidates comparisons unless backlog behavior is the explicit subject of the test.
