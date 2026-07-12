from pathlib import Path

UPSTREAM_COMMIT = "577c34cead6c559eec3cda0bf002c45b19f8500b"


def replace_once(path: Path, old: str, new: str, description: str) -> None:
    source = path.read_text()
    if old not in source:
        raise SystemExit(f"{description} target not found in {path}")
    path.write_text(source.replace(old, new, 1))


replace_once(
    Path("third_party/dart_amqp/lib/src/client/impl/channel_impl.dart"),
    """    // Multi Ack/Nack; messages up to seqNo
    for (var pendingSeqNo in _pendingDeliveries.keys) {
      if (pendingSeqNo > seqNo) {
        // only interested in keys up to pendingSeqNo
        break;
      }

      _PublishNotificationImpl? notification = _pendingDeliveries.remove(seqNo);
      if (notification != null &&
          _publishNotificationStream.hasListener &&
          !_publishNotificationStream.isClosed) {
        notification.published = ack;
        _publishNotificationStream.add(notification);
      }
    }

    return;""",
    """    // Multi Ack/Nack; messages up to seqNo. Snapshot matching keys before
    // mutating the map because Map.keys is a live iterable.
    final confirmedSequenceNumbers = _pendingDeliveries.keys
        .where((pendingSeqNo) => pendingSeqNo <= seqNo)
        .toList(growable: false);

    for (final pendingSeqNo in confirmedSequenceNumbers) {
      final notification = _pendingDeliveries.remove(pendingSeqNo);
      if (notification != null &&
          _publishNotificationStream.hasListener &&
          !_publishNotificationStream.isClosed) {
        notification.published = ack;
        _publishNotificationStream.add(notification);
      }
    }

    return;""",
    "publisher-confirm",
)

replace_once(
    Path("third_party/dart_amqp/lib/src/client/impl/client_impl.dart"),
    """  void _handleException(ex) {
    // Ignore exceptions while shutting down
    if (_clientClosed != null) {
      return;
    }
""",
    """  void _handleException(ex) {
    // The decoder can surface Error objects as well as Exception objects.
    // Normalize them before publishing to the Exception-typed error stream.
    if (ex is! Exception) {
      ex = FatalException(
          "Unhandled AMQP client error (${ex.runtimeType}): $ex");
    }

    // Ignore exceptions while shutting down
    if (_clientClosed != null) {
      return;
    }
""",
    "client error-normalization",
)

replace_once(
    Path("packages/podbus_rabbitmq/pubspec.yaml"),
    "  dart_amqp: ^0.3.1\n",
    "  dart_amqp:\n    path: ../../third_party/dart_amqp\n",
    "podbus_rabbitmq dependency",
)

Path("third_party/dart_amqp/PODBUS_PATCHES.md").write_text(
    f"""# PodBus patches for dart_amqp

Upstream: https://github.com/achilleasa/dart_amqp

Pinned commit: `{UPSTREAM_COMMIT}` (`0.3.1`).

PodBus carries two narrow compatibility fixes:

1. Publisher multi-ack handling snapshots matching sequence numbers before mutating `_pendingDeliveries`, and removes `pendingSeqNo` instead of repeatedly removing the aggregate `seqNo`.
2. Internal non-`Exception` errors are normalized before being emitted through the package's `StreamController<Exception>`.

The vendored package intentionally stays outside the PodBus pub workspace. Its legacy development constraints must not participate in the workspace-wide solver; PodBus consumes it only through the pinned path dependency above.

The vendored copy is temporary. Replace it with an upstream release once equivalent fixes are published and the RabbitMQ stress qualification passes against that release.
"""
)
