import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:test/test.dart';

void main() {
  group('NatsMessageBus', () {
    test('publishes encoded payloads with message headers', () async {
      final adapter = FakeNatsClientAdapter();
      final bus = NatsMessageBus(config: _config(), clientAdapter: adapter);
      await bus.connect();

      await bus.publish('leads.created', {
        'leadId': 7,
      }, headers: MessageHeaders(correlationId: 'corr-1'));

      expect(adapter.published.single.subject, 'leads.created');
      expect(adapter.published.single.headers['correlationId'], 'corr-1');
      expect(
        adapter.published.single.headers['podbus-content-type'],
        'application/json',
      );
      await bus.close();
    });

    test('subscribes and decodes incoming messages', () async {
      final adapter = FakeNatsClientAdapter();
      final bus = NatsMessageBus(config: _config(), clientAdapter: adapter);
      await bus.connect();

      final received = Completer<Map<String, Object?>>();
      await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        queueGroup: 'lead-workers',
        handler: (context, payload) async {
          expect(context.subject, 'leads.created');
          expect(context.headers.correlationId, 'corr-1');
          received.complete(payload);
        },
      );

      adapter.emit(
        'leads.created',
        NatsClientMessage(
          subject: 'leads.created',
          bytes: '{"leadId":7}'.codeUnits,
          headers: {
            'correlationId': 'corr-1',
            'podbus-content-type': 'application/json',
            'podbus-schema-version': '1',
          },
        ),
      );

      expect(await received.future.timeout(Duration(seconds: 1)), {
        'leadId': 7,
      });
      expect(adapter.subscriptions.single.queueGroup, 'lead-workers');
      await bus.close();
    });

    test('supports request reply through the adapter', () async {
      final adapter = FakeNatsClientAdapter()
        ..nextResponse = NatsClientMessage(
          subject: 'lead.score',
          bytes: '{"score":91}'.codeUnits,
          headers: {
            'podbus-content-type': 'application/json',
            'podbus-schema-version': '1',
          },
        );
      final bus = NatsMessageBus(config: _config(), clientAdapter: adapter);
      await bus.connect();

      final response = await bus
          .request<Map<String, Object?>, Map<String, Object?>>('lead.score', {
            'leadId': 7,
          }, timeout: Duration(seconds: 1));

      expect(response, {'score': 91});
      expect(adapter.requests.single.subject, 'lead.score');
      await bus.close();
    });
  });
}

NatsMessagingConfig _config() {
  return NatsMessagingConfig(servers: [Uri.parse('nats://localhost:4222')]);
}

final class FakeNatsClientAdapter implements NatsClientAdapter {
  final published = <_Published>[];
  final requests = <_Request>[];
  final subscriptions = <_Subscription>[];
  NatsClientMessage? nextResponse;
  var connected = false;

  @override
  bool get isConnected => connected;

  @override
  Future<void> close() async {
    connected = false;
  }

  @override
  Future<void> connect(NatsMessagingConfig config) async {
    connected = true;
  }

  @override
  Future<void> drain() async {
    connected = false;
  }

  @override
  Future<void> flush() async {}

  @override
  Future<void> publish(
    String subject,
    List<int> bytes, {
    String? replyTo,
    Map<String, String> headers = const {},
  }) async {
    published.add(_Published(subject, bytes, headers, replyTo));
  }

  @override
  Future<NatsClientMessage> request(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    Map<String, String> headers = const {},
  }) async {
    requests.add(_Request(subject, bytes, headers));
    return nextResponse!;
  }

  @override
  NatsClientSubscription subscribe(String subject, {String? queueGroup}) {
    final subscription = _Subscription(subject, queueGroup);
    subscriptions.add(subscription);
    return subscription;
  }

  void emit(String subject, NatsClientMessage message) {
    subscriptions
        .singleWhere((item) => item.subject == subject)
        .controller
        .add(message);
  }
}

final class _Published {
  const _Published(this.subject, this.bytes, this.headers, this.replyTo);

  final String subject;
  final List<int> bytes;
  final Map<String, String> headers;
  final String? replyTo;
}

final class _Request {
  const _Request(this.subject, this.bytes, this.headers);

  final String subject;
  final List<int> bytes;
  final Map<String, String> headers;
}

final class _Subscription implements NatsClientSubscription {
  _Subscription(this.subject, this.queueGroup);

  final String subject;
  final String? queueGroup;
  final controller = StreamController<NatsClientMessage>.broadcast();

  @override
  Stream<NatsClientMessage> get messages => controller.stream;

  @override
  Future<void> close() async {
    await controller.close();
  }
}
