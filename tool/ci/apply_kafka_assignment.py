from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"Expected fragment not found in {path}: {old[:100]!r}")
    file.write_text(text.replace(old, new))


bindings = "packages/podbus_kafka/lib/src/rdkafka_bindings.dart"
replace(
    bindings,
    """final class RdKafkaTopicPartitionList extends ffi.Opaque {}

final class RdKafkaTopicPartition extends ffi.Opaque {}
""",
    """final class RdKafkaTopicPartitionList extends ffi.Struct {
  @ffi.Int32()
  external int count;

  @ffi.Int32()
  external int size;

  external ffi.Pointer<RdKafkaTopicPartition> elements;
}

final class RdKafkaTopicPartition extends ffi.Struct {
  external ffi.Pointer<ffi.Char> topic;

  @ffi.Int32()
  external int partition;

  @ffi.Int64()
  external int offset;

  external ffi.Pointer<ffi.Void> metadata;

  @ffi.Size()
  external int metadataSize;

  external ffi.Pointer<ffi.Void> opaque;

  @ffi.Int32()
  external int error;

  external ffi.Pointer<ffi.Void> privateData;
}
""",
)
replace(
    bindings,
    """  late final int Function(ffi.Pointer<RdKafkaHandle>) rdKafkaConsumerClose =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>),
        int Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_consumer_close');

  late final ffi.Pointer<ffi.Char> Function(int) rdKafkaErrorString = _library
""",
    """  late final int Function(ffi.Pointer<RdKafkaHandle>) rdKafkaConsumerClose =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>),
        int Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_consumer_close');

  late final int Function(
    ffi.Pointer<RdKafkaHandle>,
    ffi.Pointer<ffi.Pointer<RdKafkaTopicPartitionList>>,
  ) rdKafkaAssignment = _library.lookupFunction<
    ffi.Int32 Function(
      ffi.Pointer<RdKafkaHandle>,
      ffi.Pointer<ffi.Pointer<RdKafkaTopicPartitionList>>,
    ),
    int Function(
      ffi.Pointer<RdKafkaHandle>,
      ffi.Pointer<ffi.Pointer<RdKafkaTopicPartitionList>>,
    )
  >('rd_kafka_assignment');

  late final ffi.Pointer<ffi.Char> Function(int) rdKafkaErrorString = _library
""",
)

client = "packages/podbus_kafka/lib/src/native_kafka_client.dart"
replace(
    client,
    """  ffi.Pointer<RdKafkaHandle>? _kafka;
  ffi.Pointer<RdKafkaMessage>? _pendingMessage;
""",
    """  ffi.Pointer<RdKafkaHandle>? _kafka;
  ffi.Pointer<RdKafkaMessage>? _pendingMessage;
  ffi.Pointer<RdKafkaMessage>? _bufferedMessage;
""",
)
replace(
    client,
    """  NativeKafkaRecord? poll(Duration timeout) {
    _releasePendingMessage();

    final message = _bindings.rdKafkaConsumerPoll(
      _requireKafka(),
      timeout.inMilliseconds,
    );
""",
    """  void waitForAssignment(Duration timeout) {
    if (timeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Kafka assignment timeout must be greater than zero.',
      );
    }

    final kafka = _requireKafka();
    final deadline = DateTime.now().add(timeout);
    final assignment = calloc<ffi.Pointer<RdKafkaTopicPartitionList>>();
    try {
      while (DateTime.now().isBefore(deadline)) {
        final event = _bindings.rdKafkaConsumerPoll(kafka, 100);
        if (event != ffi.nullptr) {
          if (event.ref.error == rdKafkaRespErrNoError) {
            _bufferedMessage = event;
            return;
          }
          _bindings.rdKafkaMessageDestroy(event);
        }

        assignment.value = ffi.nullptr;
        final result = _bindings.rdKafkaAssignment(kafka, assignment);
        _ensureNoError(
          _bindings,
          result,
          'Kafka assignment lookup failed.',
        );
        final current = assignment.value;
        if (current != ffi.nullptr) {
          final assigned = current.ref.count > 0;
          _bindings.rdKafkaTopicPartitionListDestroy(current);
          assignment.value = ffi.nullptr;
          if (assigned) {
            return;
          }
        }
      }
    } finally {
      final current = assignment.value;
      if (current != ffi.nullptr) {
        _bindings.rdKafkaTopicPartitionListDestroy(current);
      }
      calloc.free(assignment);
    }

    throw MessagingTimeoutException(
      'Kafka consumer did not receive a partition assignment within $timeout.',
      timeout: timeout,
    );
  }

  NativeKafkaRecord? poll(Duration timeout) {
    _releasePendingMessage();

    final buffered = _bufferedMessage;
    _bufferedMessage = null;
    final message = buffered ??
        _bindings.rdKafkaConsumerPoll(
          _requireKafka(),
          timeout.inMilliseconds,
        );
""",
)
replace(
    client,
    """    _releasePendingMessage();
    _bindings.rdKafkaUnsubscribe(kafka);
""",
    """    _releasePendingMessage();
    final buffered = _bufferedMessage;
    if (buffered != null) {
      _bindings.rdKafkaMessageDestroy(buffered);
      _bufferedMessage = null;
    }
    _bindings.rdKafkaUnsubscribe(kafka);
""",
)

adapter = "packages/podbus_kafka/lib/src/kafka_adapter.dart"
replace(
    adapter,
    """    final consumer = NativeKafkaConsumer.connect(
      bindings: _nativeLibrary.open(),
      topics: topics,
      properties: _consumerProperties(config, groupId),
    );
    final adapterConsumer = _DartKafkaAdapterConsumer(consumer);
""",
    """    final consumer = NativeKafkaConsumer.connect(
      bindings: _nativeLibrary.open(),
      topics: topics,
      properties: _consumerProperties(config, groupId),
    );
    consumer.waitForAssignment(config.requestTimeout);
    final adapterConsumer = _DartKafkaAdapterConsumer(consumer);
""",
)

print("Kafka assignment readiness patch applied.")
