// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart' as nats;

import 'config.dart';

abstract interface class NatsJetStreamAdapter {
  bool get isConnected;

  Future<void> connect(NatsMessagingConfig config);

  Future<void> drain();

  Future<void> close();

  Future<void> flush();

  Future<void> createOrUpdateStream(NatsJetStreamConfig config);

  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
  });

  Future<NatsJetStreamPublishAck> publish(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    String? messageId,
    Map<String, String> headers,
  });
}

abstract interface class NatsJetStreamConsumer {
  Future<List<NatsJetStreamMessage>> fetch({
    required int batch,
    required Duration timeout,
  });
}

abstract interface class NatsJetStreamMessage {
  String get subject;

  List<int> get bytes;

  Map<String, String> get headers;

  int get deliveryCount;

  int? get streamSequence;

  int? get consumerSequence;

  Future<bool> ack();

  Future<bool> nak({Duration? delay});

  Future<bool> term();

  Future<bool> inProgress();
}

final class NatsJetStreamPublishAck {
  const NatsJetStreamPublishAck({
    required this.stream,
    required this.sequence,
    required this.duplicate,
  });

  final String stream;
  final int sequence;
  final bool duplicate;
}

final class DartNatsJetStreamAdapter implements NatsJetStreamAdapter {
  DartNatsJetStreamAdapter({nats.Client? client})
    : _client = client ?? nats.Client();

  final nats.Client _client;
  nats.JetStream? _jetStream;

  @override
  bool get isConnected => _client.connected;

  @override
  Future<void> close() => _client.close();

  @override
  Future<void> connect(NatsMessagingConfig config) async {
    final connectOption = nats.ConnectOption(
      authToken: config.token,
      user: config.username,
      pass: config.password,
      tlsRequired: config.useTls,
      headers: true,
      name: 'podbus-jetstream',
    );

    await _client.connect(
      config.servers.first,
      servers: config.servers.length > 1
          ? config.servers.skip(1).toList()
          : null,
      connectOption: connectOption,
      timeout: config.connectTimeout.inSeconds,
      retry: true,
      retryCount: 3,
    );
    _jetStream = nats.JetStream(_client);
  }

  @override
  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
  }) async {
    final consumer = await _js.createOrUpdateConsumer<dynamic>(
      streamName,
      nats.ConsumerConfig(
        durable: consumerName,
        filterSubject: topic,
        ackPolicy: 'explicit',
        deliverPolicy: 'all',
      ),
    );
    return _DartNatsJetStreamConsumer(consumer);
  }

  @override
  Future<void> createOrUpdateStream(NatsJetStreamConfig config) async {
    await _js.createOrUpdateStream(
      nats.StreamConfig(
        name: config.streamName,
        subjects: config.subjects,
        storage: _storage(config.storage),
        retention: _retention(config.retentionPolicy),
        maxMsgs: config.maxMsgs ?? -1,
        maxAge: config.maxAge,
        numReplicas: config.replicas,
      ),
    );
  }

  @override
  Future<void> drain() => _client.drain();

  @override
  Future<void> flush() => _client.flush();

  @override
  Future<NatsJetStreamPublishAck> publish(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    String? messageId,
    Map<String, String> headers = const {},
  }) async {
    final ack = await _js.publish(
      subject,
      Uint8List.fromList(bytes),
      timeout: timeout,
      opts: messageId == null ? null : nats.PubOpts(msgId: messageId),
      header: nats.Header(headers: headers),
    );
    return NatsJetStreamPublishAck(
      stream: ack.stream,
      sequence: ack.sequence,
      duplicate: ack.duplicate,
    );
  }

  nats.JetStream get _js {
    final jetStream = _jetStream;
    if (jetStream == null) {
      throw StateError('NATS JetStream adapter is not connected.');
    }
    return jetStream;
  }
}

final class _DartNatsJetStreamConsumer implements NatsJetStreamConsumer {
  const _DartNatsJetStreamConsumer(this._consumer);

  final nats.Consumer<dynamic> _consumer;

  @override
  Future<List<NatsJetStreamMessage>> fetch({
    required int batch,
    required Duration timeout,
  }) async {
    final messages = await _consumer.fetch(batch: batch, timeout: timeout);
    return [for (final message in messages) _DartNatsJetStreamMessage(message)];
  }
}

final class _DartNatsJetStreamMessage implements NatsJetStreamMessage {
  const _DartNatsJetStreamMessage(this._message);

  final nats.Message<dynamic> _message;

  @override
  String get subject => _message.subject ?? '';

  @override
  List<int> get bytes => _message.byte;

  @override
  Map<String, String> get headers {
    return Map.unmodifiable(
      _message.header?.headers ?? const <String, String>{},
    );
  }

  @override
  int get deliveryCount => _deliveryCount(_message.replyTo);

  @override
  int? get consumerSequence => _message.consumerSequence;

  @override
  int? get streamSequence => _message.streamSequence;

  @override
  Future<bool> ack() async => _message.ack();

  @override
  Future<bool> inProgress() async => _message.inProgress();

  @override
  Future<bool> nak({Duration? delay}) async {
    if (delay != null && delay > Duration.zero) {
      await Future<void>.delayed(delay);
    }
    return _message.nak();
  }

  @override
  Future<bool> term() async => _message.term();
}

String _storage(NatsJetStreamStorage storage) {
  return switch (storage) {
    NatsJetStreamStorage.file => 'file',
    NatsJetStreamStorage.memory => 'memory',
  };
}

String _retention(NatsJetStreamRetentionPolicy? retentionPolicy) {
  return switch (retentionPolicy) {
    null || NatsJetStreamRetentionPolicy.limits => 'limits',
    NatsJetStreamRetentionPolicy.interest => 'interest',
    NatsJetStreamRetentionPolicy.workQueue => 'workqueue',
  };
}

int _deliveryCount(String? replyTo) {
  if (replyTo == null || replyTo.isEmpty) {
    return 1;
  }
  final parts = replyTo.split('.');
  if (parts.length < 5 || parts[0] != r'$JS' || parts[1] != 'ACK') {
    return 1;
  }
  return int.tryParse(parts[4]) ?? 1;
}

String encodeNatsDelayedNak(Duration delay) {
  return '-NAK ${jsonEncode({'delay': delay.inMicroseconds * 1000})}';
}
