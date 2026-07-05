import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_example/src/podbus/lead_service.dart';
import 'package:serverpod/serverpod.dart';
import 'package:test/test.dart';

void main() {
  test('creates a lead and enqueues the welcome workflow', () async {
    final bus = InMemoryMessageBus();
    final queue = InMemoryDurableJobQueue(
      idempotencyStore: InMemoryIdempotencyStore(),
    );
    await bus.connect();
    await queue.connect();

    final service = LeadService(
      session: _FakeSession(),
      bus: bus,
      queue: queue,
    );

    final lead = await service.createLead('lead@example.com');

    expect(lead.email, 'lead@example.com');
    await queue.close();
    await bus.close();
  });
}

final class _FakeSession implements Session {
  final logs = <String>[];

  @override
  void log(
    String message, {
    LogLevel? level,
    Object? exception,
    StackTrace? stackTrace,
  }) {
    logs.add(message);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
