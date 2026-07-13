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
    required NatsJetStreamConsumerConfig config,
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
  final Map<String, Completer<nats.Message<dynamic>>> _pendingPublishes = {};
  nats.JetStream? _jetStream;
  nats.Subscription<dynamic>? _publishReplySubscription;
  // Managed by _closePublishInbox during close, drain, and reconnect.
  // ignore: cancel_subscriptions
  StreamSubscription<nats.Message<dynamic>>? _publishReplyListener;
  String? _publishInboxPrefix;
  int _publishSequence = 0;
  bool _closing = false;

  @override
  bool get isConnected => _client.connected && !_closing;

  @override
  Future<void> close() async {
    _closing = true;
    await _closePublishInbox(
      StateError('NATS JetStream adapter closed before publish confirmation.'),
    );
    _jetStream = null;
    await _client.close();
  }

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

    _closing = false;
    try {
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
      await _openPublishInbox();
    } on Object {
      _jetStream = null;
      await _closePublishInbox(
        StateError('NATS JetStream adapter failed while connecting.'),
      );
      await _client.close();
      rethrow;
    }
  }

  @override
  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
    required NatsJetStreamConsumerConfig config,
  }) async {
    final consumer = await _js.createOrUpdateConsumer<dynamic>(
      streamName,
      _PodBusConsumerConfig(
        durable: consumerName,
        filterSubject: topic,
        ackWait: config.ackWait,
        maxDeliver: config.maxDeliver,
        maxAckPending: config.maxAckPending,
        idleHeartbeat: config.idleHeartbeat,
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
  Future<void> drain() async {
    if (_pendingPublishes.isNotEmpty) {
      await Future.wait([
        for (final pending in _pendingPublishes.values.toList()) pending.future,
      ]);
    }
    await _client.flush();
    _closing = true;
    await _closePublishInbox(
      StateError('NATS JetStream adapter drained before publish confirmation.'),
    );
    _jetStream = null;
    await _client.drain();
  }

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
    final inboxPrefix = _publishInboxPrefix;
    if (!isConnected || inboxPrefix == null) {
      throw StateError('NATS JetStream adapter is not connected.');
    }

    final replyTo = '$inboxPrefix.${_publishSequence++}';
    final responseCompleter = Completer<nats.Message<dynamic>>();
    _pendingPublishes[replyTo] = responseCompleter;
    final wireHeaders = <String, String>{...headers, 'Nats-Msg-Id': ?messageId};

    try {
      final sent = await _client.pub(
        subject,
        Uint8List.fromList(bytes),
        replyTo: replyTo,
        buffer: false,
        header: nats.Header(headers: wireHeaders),
      );
      if (!sent) {
        throw StateError('NATS JetStream publish could not be written.');
      }

      final response = await responseCompleter.future.timeout(
        timeout,
        onTimeout: () => throw TimeoutException(
          'NATS JetStream publish confirmation exceeded $timeout.',
        ),
      );
      final status = response.header?.status;
      if (status != null && status >= 300) {
        throw StateError(
          'NATS JetStream publish failed with status $status: '
          '${response.string}',
        );
      }

      final decoded = jsonDecode(response.string);
      if (decoded is! Map<String, dynamic>) {
        throw StateError('NATS JetStream returned an invalid publish ack.');
      }
      final error = decoded['error'];
      if (error != null) {
        final description = error is Map<String, dynamic>
            ? error['description'] ?? error.toString()
            : error.toString();
        throw StateError('NATS JetStream publish failed: $description');
      }
      final stream = decoded['stream'];
      final sequence = decoded['seq'];
      if (stream is! String || sequence is! int) {
        throw StateError('NATS JetStream returned an incomplete publish ack.');
      }
      return NatsJetStreamPublishAck(
        stream: stream,
        sequence: sequence,
        duplicate: decoded['duplicate'] as bool? ?? false,
      );
    } finally {
      _pendingPublishes.remove(replyTo);
    }
  }

  Future<void> _openPublishInbox() async {
    await _closePublishInbox(
      StateError('NATS JetStream publish inbox was replaced.'),
    );
    final nonce = DateTime.now().microsecondsSinceEpoch;
    final identity = identityHashCode(this).abs();
    final inboxPrefix = '_INBOX.PODBUS.JS.$nonce.$identity';
    final subscription = _client.sub<dynamic>('$inboxPrefix.>');
    _publishInboxPrefix = inboxPrefix;
    _publishReplySubscription = subscription;
    _publishReplyListener = subscription.stream.listen(
      _handlePublishReply,
      onError: (Object error, StackTrace stackTrace) {
        _failPendingPublishes(error, stackTrace);
      },
      onDone: () {
        if (!_closing) {
          _failPendingPublishes(
            StateError('NATS JetStream publish reply inbox closed.'),
            StackTrace.current,
          );
        }
      },
    );
    await _client.flush();
  }

  void _handlePublishReply(nats.Message<dynamic> message) {
    final subject = message.subject;
    if (subject == null) {
      return;
    }
    final completer = _pendingPublishes.remove(subject);
    if (completer != null && !completer.isCompleted) {
      completer.complete(message);
    }
  }

  void _failPendingPublishes(Object error, StackTrace stackTrace) {
    final pending = _pendingPublishes.values.toList();
    _pendingPublishes.clear();
    for (final completer in pending) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace);
      }
    }
  }

  Future<void> _closePublishInbox(Object error) async {
    final listener = _publishReplyListener;
    final subscription = _publishReplySubscription;
    _publishReplyListener = null;
    _publishReplySubscription = null;
    _publishInboxPrefix = null;
    _failPendingPublishes(error, StackTrace.current);
    await listener?.cancel();
    if (subscription != null) {
      _client.unSub(subscription);
    }
  }

  nats.JetStream get _js {
    final jetStream = _jetStream;
    if (jetStream == null) {
      throw StateError('NATS JetStream adapter is not connected.');
    }
    return jetStream;
  }
}

final class _PodBusConsumerConfig extends nats.ConsumerConfig {
  _PodBusConsumerConfig({
    required super.durable,
    required super.filterSubject,
    required this.ackWait,
    required this.maxDeliver,
    required this.maxAckPending,
    super.idleHeartbeat,
  }) : super(ackPolicy: 'explicit', deliverPolicy: 'all');

  final Duration ackWait;
  final int maxDeliver;
  final int maxAckPending;

  @override
  Map<String, dynamic> toJson() {
    return {
      ...super.toJson(),
      'ack_wait': ackWait.inMicroseconds * 1000,
      'max_deliver': maxDeliver,
      'max_ack_pending': maxAckPending,
    };
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
