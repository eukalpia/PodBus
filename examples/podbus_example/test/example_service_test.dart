import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_example/example_service.dart';
import 'package:test/test.dart';

void main() {
  test(
    'creates a lead, publishes an event, enqueues a job, and scores the lead',
    () async {
      final bus = InMemoryMessageBus();
      final queue = InMemoryDurableJobQueue(
        idempotencyStore: InMemoryIdempotencyStore(),
      );

      await bus.connect();
      await queue.connect();
      await LeadWorkers(bus: bus, queue: queue).start();

      final service = LeadService(bus: bus, queue: queue);
      final lead = await service.createLead('lead@example.com');
      final score = await service.requestLeadScoring(lead);

      expect(lead.email, 'lead@example.com');
      expect(score.leadId, lead.id);
      expect(score.score, 80);

      await queue.close();
      await bus.close();
    },
  );
}
