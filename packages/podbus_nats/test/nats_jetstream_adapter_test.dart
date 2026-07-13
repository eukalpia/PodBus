import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart' as nats;
import 'package:podbus_nats/src/config.dart';
import 'package:podbus_nats/src/nats_jetstream_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('DartNatsJetStreamAdapter publish acknowledgements', () {
    test('routes concurrent out-of-order acknowledgements', () async {
      final client = _FakeNatsClient();
      final adapter = DartNatsJetStreamAdapter(client: client);
      await adapter.connect(_config());
      addTearDown(adapter.close);

      final acknowledgements = await Future.wait([
        for (var index = 0; index < 64; index += 1)
          adapter.publish(
            'podbus.test.concurrent',
            utf8.encode('$index'),
            timeout: const Duration(seconds: 1),
            messageId: 'message-$index',
          ),
      ]);

      expect(client.maximumInFlight, greaterThan(1));
      expect(
        acknowledgements.map((ack) => ack.sequence).toSet(),
        hasLength(64),
      );
      expect(client.messageIds, hasLength(64));
      expect(client.messageIds, contains('message-0'));
      expect(client.messageIds, contains('message-63'));
    });

    test('a failed publish does not poison later confirmations', () async {
      final client = _FakeNatsClient()..failNext = true;
      final adapter = DartNatsJetStreamAdapter(client: client);
      await adapter.connect(_config());
      addTearDown(adapter.close);

      await expectLater(
        adapter.publish(
          'podbus.test.failure',
          const [1],
          timeout: const Duration(seconds: 1),
        ),
        throwsA(isA<StateError>()),
      );

      final acknowledgement = await adapter.publish(
        'podbus.test.recovery',
        const [2],
        timeout: const Duration(seconds: 1),
      );
      expect(acknowledgement.stream, 'PODBUS_TEST');
      expect(acknowledgement.sequence, greaterThan(0));
    });

    test('close fails an outstanding confirmation immediately', () async {
      final client = _FakeNatsClient()..autoReply = false;
      final adapter = DartNatsJetStreamAdapter(client: client);
      await adapter.connect(_config());

      final pending = adapter.publish(
        'podbus.test.pending',
        const [3],
        timeout: const Duration(minutes: 1),
      );
      await Future<void>.delayed(Duration.zero);
      await adapter.close();

      await expectLater(pending, throwsA(isA<StateError>()));
    });
  });
}

NatsMessagingConfig _config() {
  return NatsMessagingConfig(
    servers: [Uri.parse('nats://fake:4222')],
    connectTimeout: const Duration(seconds: 1),
    requestTimeout: const Duration(seconds: 1),
  );
}

final class _FakeNatsClient extends nats.Client {
  bool _connected = false;
  int _sid = 0;
  int _sequence = 0;
  int _inFlight = 0;
  nats.Subscription<dynamic>? _replySubscription;

  bool autoReply = true;
  bool failNext = false;
  int maximumInFlight = 0;
  final Set<String> messageIds = {};

  @override
  bool get connected => _connected;

  @override
  Future<void> connect(
    Uri uri, {
    List<Uri>? servers,
    nats.ConnectOption? connectOption,
    int timeout = 5,
    bool retry = true,
    int retryInterval = 10,
    int retryCount = 3,
    SecurityContext? securityContext,
  }) async {
    _connected = true;
  }

  @override
  nats.Subscription<T> sub<T>(
    String subject, {
    String? queueGroup,
    T Function(String)? jsonDecoder,
  }) {
    final subscription = nats.Subscription<T>(
      ++_sid,
      subject,
      this,
      queueGroup: queueGroup,
      jsonDecoder: jsonDecoder,
    );
    _replySubscription = subscription as nats.Subscription<dynamic>;
    return subscription;
  }

  @override
  Future<bool> pub(
    String? subject,
    Uint8List data, {
    String? replyTo,
    bool? buffer,
    nats.Header? header,
  }) async {
    final messageId = header?.get('Nats-Msg-Id');
    if (messageId != null) {
      messageIds.add(messageId);
    }
    if (replyTo == null || !autoReply) {
      return true;
    }

    _inFlight += 1;
    if (_inFlight > maximumInFlight) {
      maximumInFlight = _inFlight;
    }
    final sequence = ++_sequence;
    final shouldFail = failNext;
    failNext = false;
    final delay = Duration(milliseconds: sequence.isEven ? 1 : 4);
    unawaited(
      Future<void>.delayed(delay, () {
        final subscription = _replySubscription;
        if (subscription == null) {
          return;
        }
        final payload = shouldFail
            ? {
                'error': {'description': 'simulated publish failure'},
              }
            : {
                'stream': 'PODBUS_TEST',
                'seq': sequence,
                'duplicate': false,
              };
        subscription.add(
          nats.Message<dynamic>(
            replyTo,
            subscription.sid,
            Uint8List.fromList(utf8.encode(jsonEncode(payload))),
            this,
          ),
        );
        _inFlight -= 1;
      }),
    );
    return true;
  }

  @override
  Future<void> flush() async {}

  @override
  Future<void> drain() async {
    _connected = false;
  }

  @override
  Future<void> close() async {
    _connected = false;
  }
}