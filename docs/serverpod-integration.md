# Serverpod Integration

Serverpod integration lives in `podbus_serverpod`, not in `podbus_core`.

Current bridge:

- `ServerpodMessaging<TSession>`
- generic async session factory
- session-aware message handlers
- session-aware job handlers
- logging adapter hooks
- config loader for local environment variables
- lifecycle `start()` and `stop()`
- endpoint and worker examples in `examples/podbus_example`

Known gaps:

- Serverpod `Session` convenience typedefs
- production auth/rate-limit guidance for example endpoints
- failure-atomic startup and rollback tests
- integration tests that exercise generated Serverpod endpoint helpers
- production config examples for TLS/SASL broker connections

This repository does not modify Serverpod core.
