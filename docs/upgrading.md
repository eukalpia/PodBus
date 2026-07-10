# Upgrading PodBus

PodBus is alpha software. Pin the exact Git commit or release tag and review every changelog entry before upgrading.

## Safe upgrade sequence

1. Read `CHANGELOG.md` and compare the transport capability matrix.
2. Run `dart pub get`, formatting, analysis, and all unit tests.
3. Run Docker-backed integration tests against the broker versions used in production.
4. Deploy consumers that can read both the old and new wire schemas.
5. Deploy producers that write the new schema.
6. Wait through the maximum broker retention or replay window before removing old decoders.
7. Upgrade one service replica first and inspect errors, lag, retries, and dead letters.
8. Keep the previous image and database migration rollback plan available.

## Wire compatibility

Treat `messageType` and `schemaVersion` as a public contract.

- Add fields as optional or supply defaults.
- Never reuse a message type for a different meaning.
- Upcast old versions explicitly in the codec registry.
- Reject unknown future versions instead of guessing.
- Keep compatibility tests with serialized fixtures from every supported version.

Changing a Dart class without changing the wire version does not make the change compatible. The broker stores bytes, not intentions.

## Database migrations

Apply additive outbox and inbox migrations before deploying code that depends on them. Avoid long table rewrites on active queues. For destructive changes:

1. deploy code that no longer reads the old column;
2. wait for old replicas and leases to disappear;
3. back up the affected tables;
4. apply the destructive migration;
5. verify the outbox relay and inbox acquisition paths.

## Durable consumer names

A durable name represents processing state. Renaming it can create a new consumer and replay retained messages. Treat durable-name changes as data migrations and document whether replay is expected.

## Broker upgrades

Test the current and next broker versions in CI or a staging environment. Validate:

- connection and authentication;
- topology declarations;
- publisher confirms or delivery reports;
- acknowledgement and commit behavior;
- retry and dead-letter routing;
- reconnect and graceful shutdown;
- metrics and health checks.

## Rollback

A rollback is safe only while the previous version can understand messages produced by the new version. When that is not true, stop producers, drain or quarantine new-version messages, then roll back consumers and producers together.
