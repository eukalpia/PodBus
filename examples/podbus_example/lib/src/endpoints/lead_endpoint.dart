import 'package:podbus_example/src/generated/protocol.dart';
import 'package:podbus_example/src/podbus/lead_service.dart';
import 'package:podbus_example/src/podbus/runtime.dart';
import 'package:serverpod/serverpod.dart';

class LeadEndpoint extends Endpoint {
  Future<Lead> createLead(Session session, String email) {
    return _service(session).createLead(email);
  }

  Future<void> publishLeadCreatedEvent(Session session, Lead lead) {
    return _service(session).publishLeadCreatedEvent(lead);
  }

  Future<void> enqueueWelcomeEmailJob(Session session, Lead lead) {
    return _service(session).enqueueWelcomeEmailJob(lead);
  }

  Future<LeadScore> requestLeadScoring(Session session, Lead lead) {
    return _service(session).requestLeadScoring(lead);
  }

  LeadService _service(Session session) {
    return LeadService(
      session: session,
      bus: PodBusRuntime.messaging.bus,
      queue: PodBusRuntime.messaging.queue,
    );
  }
}
