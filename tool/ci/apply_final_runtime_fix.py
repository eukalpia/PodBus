from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


resilience_path = Path("packages/podbus_core/lib/src/resilience.dart")
resilience = resilience_path.read_text()

shared_bind_block = """            for (final registration in _registrations.toList()) {
              if (!registration.isClosed) {
                await registration.bind(replacement);
              }
            }
            _delegate = replacement;
"""

message_bus_bind_block = """            for (final registration in _registrations.toList()) {
              if (!registration.isClosed) {
                await registration.bind(replacement);
              }
            }
            if (_closing || generation != _generation) {
              throw const MessagingConnectionException(
                'Message bus recovery was cancelled during registration restore.',
              );
            }
            _delegate = replacement;
"""

# The first occurrence belongs to ResilientMessageBus.
if resilience.count(shared_bind_block) != 2:
    raise SystemExit(
        "resilience recovery bind block: expected two matches, found "
        f"{resilience.count(shared_bind_block)}"
    )
resilience = resilience.replace(shared_bind_block, message_bus_bind_block, 1)

durable_bind_block = """            for (final registration in _registrations.toList()) {
              if (!registration.isClosed) {
                await registration.bind(replacement);
              }
            }
            if (_closing || generation != _generation) {
              throw const MessagingConnectionException(
                'Durable queue recovery was cancelled during worker restore.',
              );
            }
            _delegate = replacement;
"""
resilience = resilience.replace(shared_bind_block, durable_bind_block, 1)

resilience_path.write_text(resilience)

nats_path = Path("packages/podbus_nats/lib/src/nats_jetstream_adapter.dart")
nats = nats_path.read_text()

nats = replace_once(
    nats,
    "import 'package:dart_nats/dart_nats.dart' as nats;\n",
    "import 'package:dart_nats/dart_nats.dart' as nats;\n"
    "import 'package:podbus_core/podbus_core.dart';\n",
    "podbus_core import",
)

nats = replace_once(
    nats,
    """    if (!isConnected || inboxPrefix == null) {
      throw StateError('NATS JetStream adapter is not connected.');
    }
""",
    """    if (!isConnected || inboxPrefix == null) {
      throw const MessagingConnectionException(
        'NATS JetStream adapter is not connected.',
      );
    }
""",
    "disconnected publish guard",
)

nats = replace_once(
    nats,
    """      if (!sent) {
        throw StateError('NATS JetStream publish could not be written.');
      }
""",
    """      if (!sent) {
        throw const MessagingConnectionException(
          'NATS JetStream publish could not be written.',
        );
      }
""",
    "unsent publish guard",
)

nats = replace_once(
    nats,
    """      return NatsJetStreamPublishAck(
        stream: stream,
        sequence: sequence,
        duplicate: decoded['duplicate'] as bool? ?? false,
      );
    } finally {
      _pendingPublishes.remove(replyTo);
    }
""",
    """      return NatsJetStreamPublishAck(
        stream: stream,
        sequence: sequence,
        duplicate: decoded['duplicate'] as bool? ?? false,
      );
    } on TimeoutException catch (error, stackTrace) {
      throw MessagingTimeoutException(
        'NATS JetStream publish confirmation exceeded $timeout.',
        timeout: timeout,
        cause: error,
        stackTrace: stackTrace,
      );
    } on nats.NatsException catch (error, stackTrace) {
      throw MessagingConnectionException(
        'NATS JetStream connection failed while publishing.',
        cause: error,
        stackTrace: stackTrace,
      );
    } finally {
      _pendingPublishes.remove(replyTo);
    }
""",
    "publish error translation",
)

nats_path.write_text(nats)
