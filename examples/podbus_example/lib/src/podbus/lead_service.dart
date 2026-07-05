import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_example/src/generated/protocol.dart';
import 'package:serverpod/serverpod.dart';

final class LeadService {
  LeadService({required this.session, required this.bus, required this.queue});

  final Session session;
  final MessageBus bus;
  final DurableJobQueue queue;

  Future<Lead> createLead(String email) async {
    final lead = Lead(id: DateTime.now().microsecondsSinceEpoch, email: email);
    await publishLeadCreatedEvent(lead);
    await enqueueWelcomeEmailJob(lead);
    return lead;
  }

  Future<void> publishLeadCreatedEvent(Lead lead) {
    session.log('Publishing lead-created event for lead ${lead.id}.');
    return bus.publish(
      'leads.created',
      lead.toJson(),
      headers: MessageHeaders(
        correlationId: 'lead-${lead.id}',
        idempotencyKey: 'lead-created:${lead.id}',
      ),
    );
  }

  Future<void> enqueueWelcomeEmailJob(Lead lead) {
    session.log('Enqueueing welcome email job for lead ${lead.id}.');
    return queue.enqueue(
      'email.welcome',
      WelcomeEmailJob(leadId: lead.id, email: lead.email).toJson(),
      idempotencyKey: 'welcome-email:${lead.id}',
      retryPolicy: RetryPolicy(
        maxAttempts: 3,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 30),
      ),
    );
  }

  Future<LeadScore> requestLeadScoring(Lead lead) async {
    final response = await bus
        .request<Map<String, Object?>, Map<String, Object?>>(
          'lead.score',
          lead.toJson(),
          timeout: const Duration(seconds: 3),
        );
    return LeadScore.fromJson(response);
  }
}
