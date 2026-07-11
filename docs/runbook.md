# Incident runbook

The first goal during a messaging incident is to stop data loss and uncontrolled amplification. Throughput comes second.

## Broker unavailable

**Signals:** readiness fails, reconnect counters rise, publish latency times out, or consumers stop receiving work.

1. Confirm whether the failure is application-side, network-side, or broker-side.
2. Stop deployments and configuration changes.
3. Keep database transactions writing to the outbox; do not bypass the outbox with direct publishes.
4. Verify broker quorum, disk alarms, certificates, credentials, and DNS.
5. Restore connectivity before increasing retry rates.
6. Watch outbox age and size while the broker recovers.
7. Resume traffic gradually and verify that the backlog drains without exhausting downstream services.

Do not restart every consumer simultaneously. A synchronized reconnect storm is an outage sequel, not a recovery plan.

## Consumer lag is increasing

1. Compare ingress rate, processing rate, and handler duration.
2. Identify the slow subject, queue, topic, or partition.
3. Check downstream database and API latency before adding consumers.
4. Confirm that the broker is not throttling or blocked by disk pressure.
5. Increase concurrency only within tested limits and downstream capacity.
6. If the backlog threatens retention, reduce producers or extend retention first.

For Kafka, check partition distribution. More process replicas do not help when all work is concentrated in one partition.

## Dead-letter volume is increasing

1. Sample errors without exposing personal data.
2. Separate malformed or permanently invalid messages from transient failures.
3. Stop automated replay if the same failure repeats.
4. Fix the producer, schema compatibility, credentials, or downstream dependency.
5. Replay a small canary batch.
6. Confirm normal processing before increasing replay rate.

Record the original message ID, replay ID, operator, reason, and timestamp for every manual replay.

## Duplicate-delivery storm

1. Confirm acknowledgements and commits are succeeding.
2. Check whether handlers exceed visibility or acknowledgement timeouts.
3. Verify inbox leases and idempotency-key expiry.
4. Reduce concurrency if side effects are timing out under load.
5. Keep duplicate protection enabled; never disable it to make the queue appear to drain.

## Poison message

A poison message fails deterministically because its payload, schema, or business state is invalid.

1. Classify it as permanent.
2. Move it to the dead-letter destination without repeated backoff cycles.
3. Keep the original payload excluded unless access controls and retention policy allow it.
4. Correct the message or producer before replaying.
5. Add a regression test for the exact malformed shape.

## Schema mismatch

1. Compare `messageType`, `schemaVersion`, and deployed consumer versions.
2. Roll back the incompatible producer when backward compatibility was broken.
3. Add or repair an upcaster when the old event is still valid.
4. Send unknown future versions to the dead-letter destination.
5. Do not silently deserialize an unknown version into the current model.

## Outbox backlog

1. Check broker health and publisher-confirm latency.
2. Inspect the oldest pending row and recent `last_error` values.
3. Confirm relay leases are expiring and recoverable.
4. Add relay replicas only when the broker and database have spare capacity.
5. Quarantine records that reached the maximum attempt count.
6. Alert on oldest-record age, not only row count.

## Emergency shutdown

Use an emergency shutdown when continuing would create irreversible duplicate side effects or data loss.

1. Disable readiness.
2. Pause producers where supported.
3. Stop consumers from fetching new messages.
4. Allow active handlers to drain until the shutdown deadline.
5. Preserve broker and application logs.
6. Capture queue depth, consumer lag, outbox age, and deployment versions.
7. Resume with a canary replica after the root cause is understood.

## Post-incident

Document the timeline, customer impact, lost or duplicated messages, detection gap, recovery actions, and permanent fixes. Add a failure-injection test when the incident exposed a missing scenario.
