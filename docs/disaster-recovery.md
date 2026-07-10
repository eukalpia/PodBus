# Disaster recovery

PodBus coordinates delivery; it does not replace the broker or database recovery plan. Define recovery objectives for each business flow before choosing retention and replication settings.

## Recovery objectives

Document for every message class:

- recovery point objective: how much accepted work may be lost;
- recovery time objective: how long processing may be unavailable;
- source of truth: database, broker log, or external system;
- replay window and legal retention limits;
- acceptable duplicate side effects.

## PostgreSQL outbox and inbox

Back up the outbox, inbox, and idempotency tables with the same policy as the business database. They must be restored to the same consistent point in time.

After a restore:

1. keep application readiness disabled;
2. run schema migrations;
3. release expired outbox and inbox leases;
4. compare the restored database timestamp with broker retention;
5. start one outbox relay and one consumer replica;
6. verify deduplication and side effects;
7. scale gradually.

Restoring the business tables without the inbox can repeat previously completed side effects. Restoring the inbox without the business tables can suppress work that the restored business state no longer contains.

## NATS JetStream

Use replicated file-backed streams for production durability. Include stream configuration in version control. Recovery should verify stream subjects, retention, replica count, maximum delivery count, durable consumer names, and pending acknowledgements before consumers return.

For a regional loss, restore or recreate streams according to the NATS deployment model, then replay from the authoritative source or retained stream. Do not recreate a durable consumer under a different name unless replay from the beginning is intended.

## RabbitMQ

Use durable exchanges and queues, persistent messages, publisher confirms, quorum queues where appropriate, and tested definitions export. Back up definitions and credentials through the platform's supported mechanism.

After cluster recovery, verify bindings and dead-letter or retry topology before starting publishers. A healthy TCP port with missing bindings is still a message-loss machine.

## Kafka

Rely on replicated topics and the cluster's supported backup or cross-cluster replication strategy. Preserve topic configuration, ACLs, consumer group offsets, and schema metadata.

Because the PodBus Kafka adapter is experimental, disaster recovery must include an application-level replay test and a comparison between committed offsets and externally visible side effects.

## Recovery validation

A recovery exercise is complete only when:

- a known canary event reaches the expected handler;
- a durable job survives a worker restart;
- a duplicate canary is suppressed;
- a failed canary reaches the dead-letter destination;
- outbox records publish and transition to `published`;
- dashboards and alerts receive current data;
- the rollback path is still available.

Run this exercise at least after major broker upgrades, topology changes, or wire-protocol migrations.
