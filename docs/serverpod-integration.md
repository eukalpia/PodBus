# Serverpod Integration

Serverpod integration lives in `podbus_serverpod`, not in `podbus_core`.

Current bridge:

- `ServerpodMessaging<TSession>`
- generic async session factory
- session-aware message handlers
- session-aware job handlers
- lifecycle `start()` and `stop()`

Future bridge work:

- Serverpod `Session` convenience typedefs
- Serverpod logging adapter
- Serverpod config loader
- generated endpoint examples
- worker startup/shutdown wiring

This repository does not modify Serverpod core.
