import 'dart:ffi' as ffi;

const int rdKafkaProducer = 0;
const int rdKafkaConsumer = 1;
const int rdKafkaConfOk = 0;
const int rdKafkaRespErrNoError = 0;
const int rdKafkaPartitionUnassigned = -1;
const int rdKafkaMessageCopy = 0x2;

final class RdKafkaHandle extends ffi.Opaque {}

final class RdKafkaConfig extends ffi.Opaque {}

final class RdKafkaTopic extends ffi.Opaque {}

final class RdKafkaTopicConfig extends ffi.Opaque {}

final class RdKafkaTopicPartitionList extends ffi.Struct {
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

final class RdKafkaMessage extends ffi.Struct {
  @ffi.Int32()
  external int error;

  external ffi.Pointer<RdKafkaTopic> topic;

  @ffi.Int32()
  external int partition;

  external ffi.Pointer<ffi.Void> payload;

  @ffi.Size()
  external int payloadLength;

  external ffi.Pointer<ffi.Void> key;

  @ffi.Size()
  external int keyLength;

  @ffi.Int64()
  external int offset;

  external ffi.Pointer<ffi.Void> privateData;
}

final class RdkafkaBindings {
  RdkafkaBindings(ffi.DynamicLibrary library) : _library = library;

  final ffi.DynamicLibrary _library;

  late final ffi.Pointer<RdKafkaConfig> Function() rdKafkaConfigNew = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaConfig> Function(),
        ffi.Pointer<RdKafkaConfig> Function()
      >('rd_kafka_conf_new');

  late final void Function(ffi.Pointer<RdKafkaConfig>) rdKafkaConfigDestroy =
      _library.lookupFunction<
        ffi.Void Function(ffi.Pointer<RdKafkaConfig>),
        void Function(ffi.Pointer<RdKafkaConfig>)
      >('rd_kafka_conf_destroy');

  late final int Function(
    ffi.Pointer<RdKafkaConfig>,
    ffi.Pointer<ffi.Char>,
    ffi.Pointer<ffi.Char>,
    ffi.Pointer<ffi.Char>,
    int,
  )
  rdKafkaConfigSet = _library
      .lookupFunction<
        ffi.Int32 Function(
          ffi.Pointer<RdKafkaConfig>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<ffi.Char>,
          ffi.Size,
        ),
        int Function(
          ffi.Pointer<RdKafkaConfig>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<ffi.Char>,
          int,
        )
      >('rd_kafka_conf_set');

  late final ffi.Pointer<RdKafkaHandle> Function(
    int,
    ffi.Pointer<RdKafkaConfig>,
    ffi.Pointer<ffi.Char>,
    int,
  )
  rdKafkaNew = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaHandle> Function(
          ffi.Int32,
          ffi.Pointer<RdKafkaConfig>,
          ffi.Pointer<ffi.Char>,
          ffi.Size,
        ),
        ffi.Pointer<RdKafkaHandle> Function(
          int,
          ffi.Pointer<RdKafkaConfig>,
          ffi.Pointer<ffi.Char>,
          int,
        )
      >('rd_kafka_new');

  late final void Function(ffi.Pointer<RdKafkaHandle>) rdKafkaDestroy = _library
      .lookupFunction<
        ffi.Void Function(ffi.Pointer<RdKafkaHandle>),
        void Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_destroy');

  late final ffi.Pointer<RdKafkaTopic> Function(
    ffi.Pointer<RdKafkaHandle>,
    ffi.Pointer<ffi.Char>,
    ffi.Pointer<RdKafkaTopicConfig>,
  )
  rdKafkaTopicNew = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaTopic> Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<RdKafkaTopicConfig>,
        ),
        ffi.Pointer<RdKafkaTopic> Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<ffi.Char>,
          ffi.Pointer<RdKafkaTopicConfig>,
        )
      >('rd_kafka_topic_new');

  late final void Function(ffi.Pointer<RdKafkaTopic>) rdKafkaTopicDestroy =
      _library.lookupFunction<
        ffi.Void Function(ffi.Pointer<RdKafkaTopic>),
        void Function(ffi.Pointer<RdKafkaTopic>)
      >('rd_kafka_topic_destroy');

  late final ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaTopic>)
  rdKafkaTopicName = _library
      .lookupFunction<
        ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaTopic>),
        ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaTopic>)
      >('rd_kafka_topic_name');

  late final int Function(
    ffi.Pointer<RdKafkaTopic>,
    int,
    int,
    ffi.Pointer<ffi.Void>,
    int,
    ffi.Pointer<ffi.Void>,
    int,
    ffi.Pointer<ffi.Void>,
  )
  rdKafkaProduce = _library
      .lookupFunction<
        ffi.Int Function(
          ffi.Pointer<RdKafkaTopic>,
          ffi.Int32,
          ffi.Int,
          ffi.Pointer<ffi.Void>,
          ffi.Size,
          ffi.Pointer<ffi.Void>,
          ffi.Size,
          ffi.Pointer<ffi.Void>,
        ),
        int Function(
          ffi.Pointer<RdKafkaTopic>,
          int,
          int,
          ffi.Pointer<ffi.Void>,
          int,
          ffi.Pointer<ffi.Void>,
          int,
          ffi.Pointer<ffi.Void>,
        )
      >('rd_kafka_produce');

  late final int Function(ffi.Pointer<RdKafkaHandle>, int) rdKafkaPoll =
      _library.lookupFunction<
        ffi.Int Function(ffi.Pointer<RdKafkaHandle>, ffi.Int),
        int Function(ffi.Pointer<RdKafkaHandle>, int)
      >('rd_kafka_poll');

  late final int Function(ffi.Pointer<RdKafkaHandle>, int) rdKafkaFlush =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>, ffi.Int),
        int Function(ffi.Pointer<RdKafkaHandle>, int)
      >('rd_kafka_flush');

  late final int Function(ffi.Pointer<RdKafkaHandle>) rdKafkaPollSetConsumer =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>),
        int Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_poll_set_consumer');

  late final ffi.Pointer<RdKafkaTopicPartitionList> Function(int)
  rdKafkaTopicPartitionListNew = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaTopicPartitionList> Function(ffi.Int),
        ffi.Pointer<RdKafkaTopicPartitionList> Function(int)
      >('rd_kafka_topic_partition_list_new');

  late final ffi.Pointer<RdKafkaTopicPartition> Function(
    ffi.Pointer<RdKafkaTopicPartitionList>,
    ffi.Pointer<ffi.Char>,
    int,
  )
  rdKafkaTopicPartitionListAdd = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaTopicPartition> Function(
          ffi.Pointer<RdKafkaTopicPartitionList>,
          ffi.Pointer<ffi.Char>,
          ffi.Int32,
        ),
        ffi.Pointer<RdKafkaTopicPartition> Function(
          ffi.Pointer<RdKafkaTopicPartitionList>,
          ffi.Pointer<ffi.Char>,
          int,
        )
      >('rd_kafka_topic_partition_list_add');

  late final void Function(ffi.Pointer<RdKafkaTopicPartitionList>)
  rdKafkaTopicPartitionListDestroy = _library
      .lookupFunction<
        ffi.Void Function(ffi.Pointer<RdKafkaTopicPartitionList>),
        void Function(ffi.Pointer<RdKafkaTopicPartitionList>)
      >('rd_kafka_topic_partition_list_destroy');

  late final int Function(
    ffi.Pointer<RdKafkaHandle>,
    ffi.Pointer<RdKafkaTopicPartitionList>,
  )
  rdKafkaSubscribe = _library
      .lookupFunction<
        ffi.Int32 Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<RdKafkaTopicPartitionList>,
        ),
        int Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<RdKafkaTopicPartitionList>,
        )
      >('rd_kafka_subscribe');

  late final int Function(ffi.Pointer<RdKafkaHandle>) rdKafkaUnsubscribe =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>),
        int Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_unsubscribe');

  late final ffi.Pointer<RdKafkaMessage> Function(
    ffi.Pointer<RdKafkaHandle>,
    int,
  )
  rdKafkaConsumerPoll = _library
      .lookupFunction<
        ffi.Pointer<RdKafkaMessage> Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Int,
        ),
        ffi.Pointer<RdKafkaMessage> Function(ffi.Pointer<RdKafkaHandle>, int)
      >('rd_kafka_consumer_poll');

  late final void Function(ffi.Pointer<RdKafkaMessage>) rdKafkaMessageDestroy =
      _library.lookupFunction<
        ffi.Void Function(ffi.Pointer<RdKafkaMessage>),
        void Function(ffi.Pointer<RdKafkaMessage>)
      >('rd_kafka_message_destroy');

  late final ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaMessage>)
  rdKafkaMessageErrorString = _library
      .lookupFunction<
        ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaMessage>),
        ffi.Pointer<ffi.Char> Function(ffi.Pointer<RdKafkaMessage>)
      >('rd_kafka_message_errstr');

  late final int Function(
    ffi.Pointer<RdKafkaHandle>,
    ffi.Pointer<RdKafkaMessage>,
    int,
  )
  rdKafkaCommitMessage = _library
      .lookupFunction<
        ffi.Int32 Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<RdKafkaMessage>,
          ffi.Int,
        ),
        int Function(
          ffi.Pointer<RdKafkaHandle>,
          ffi.Pointer<RdKafkaMessage>,
          int,
        )
      >('rd_kafka_commit_message');

  late final int Function(ffi.Pointer<RdKafkaHandle>) rdKafkaConsumerClose =
      _library.lookupFunction<
        ffi.Int32 Function(ffi.Pointer<RdKafkaHandle>),
        int Function(ffi.Pointer<RdKafkaHandle>)
      >('rd_kafka_consumer_close');

  late final int Function(
    ffi.Pointer<RdKafkaHandle>,
    ffi.Pointer<ffi.Pointer<RdKafkaTopicPartitionList>>,
  )
  rdKafkaAssignment = _library
      .lookupFunction<
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
      .lookupFunction<
        ffi.Pointer<ffi.Char> Function(ffi.Int32),
        ffi.Pointer<ffi.Char> Function(int)
      >('rd_kafka_err2str');
}
