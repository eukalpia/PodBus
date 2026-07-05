import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_serverpod/podbus_serverpod.dart';
import 'package:test/test.dart';

void main() {
  test('wraps message handlers with a session factory', () async {
    final bus = InMemoryMessageBus();
    final queue = InMemoryDurableJobQueue();
    final bridge = ServerpodMessaging<String>(
      bus: bus,
      queue: queue,
      sessionFactory: () async => 'session-1',
    );

    await bridge.start();

    var seenSession = '';
    await bridge.subscribe<Map<String, Object?>>(
      'leads.created',
      handler: (session, context, payload) async {
        seenSession = session;
        await context.ack();
      },
    );

    await bus.publish('leads.created', {'leadId': 7});

    expect(seenSession, 'session-1');
    await bridge.stop();
  });
}
