import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_example/example_service.dart';

Future<void> main() async {
  final bus = InMemoryMessageBus();
  final queue = InMemoryDurableJobQueue(
    idempotencyStore: InMemoryIdempotencyStore(),
  );

  await bus.connect();
  await queue.connect();

  final workers = LeadWorkers(bus: bus, queue: queue);
  await workers.start();

  final service = LeadService(bus: bus, queue: queue);
  final lead = await service.createLead('lead@example.com');
  await service.requestLeadScoring(lead);

  await queue.close();
  await bus.close();
}
