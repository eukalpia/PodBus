import 'package:podbus_core/podbus_core.dart';

final class LeadService {
  LeadService({required this.bus, required this.queue});

  final MessageBus bus;
  final DurableJobQueue queue;

  Future<Lead> createLead(String email) async {
    final lead = Lead(id: 1, email: email);
    await publishLeadCreatedEvent(lead);
    await enqueueWelcomeEmailJob(lead);
    return lead;
  }

  Future<void> publishLeadCreatedEvent(Lead lead) {
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
    return queue.enqueue(
      'email.welcome',
      WelcomeEmailJob(leadId: lead.id, email: lead.email).toJson(),
      idempotencyKey: 'welcome-email:${lead.id}',
      retryPolicy: RetryPolicy(
        maxAttempts: 3,
        initialDelay: Duration(milliseconds: 100),
        maxDelay: Duration(seconds: 5),
      ),
    );
  }

  Future<LeadScore> requestLeadScoring(Lead lead) async {
    final response = await bus
        .request<Map<String, Object?>, Map<String, Object?>>(
          'lead.score',
          lead.toJson(),
          timeout: Duration(seconds: 2),
        );
    return LeadScore.fromJson(response);
  }
}

final class LeadWorkers {
  LeadWorkers({required this.bus, required this.queue});

  final MessageBus bus;
  final DurableJobQueue queue;

  Future<void> start() async {
    await bus.subscribe<Map<String, Object?>>(
      'lead.score',
      handler: (context, payload) async {
        final lead = Lead.fromJson(payload);
        await context.reply(LeadScore(leadId: lead.id, score: 80).toJson());
      },
    );

    await bus.subscribe<Map<String, Object?>>(
      'leads.created',
      queueGroup: 'lead-created-workers',
      handler: (context, payload) async {
        Lead.fromJson(payload);
        await context.ack();
      },
    );

    await queue.worker<Map<String, Object?>>(
      'email.welcome',
      durableName: 'welcome-email-worker',
      retryPolicy: RetryPolicy(
        maxAttempts: 3,
        initialDelay: Duration(milliseconds: 100),
        maxDelay: Duration(seconds: 5),
      ),
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: 'email.dead',
        includeErrorDetails: true,
      ),
      handler: (context, payload) async {
        WelcomeEmailJob.fromJson(payload);
        await context.ack();
      },
    );

    await queue.worker<Map<String, Object?>>(
      'email.dead',
      durableName: 'dead-letter-worker',
      handler: (context, payload) async {
        WelcomeEmailJob.fromJson(payload);
        await context.ack();
      },
    );
  }
}

final class Lead {
  const Lead({required this.id, required this.email});

  factory Lead.fromJson(Map<String, Object?> json) {
    return Lead(id: json['id']! as int, email: json['email']! as String);
  }

  final int id;
  final String email;

  Map<String, Object?> toJson() => {'id': id, 'email': email};
}

final class WelcomeEmailJob {
  const WelcomeEmailJob({required this.leadId, required this.email});

  factory WelcomeEmailJob.fromJson(Map<String, Object?> json) {
    return WelcomeEmailJob(
      leadId: json['leadId']! as int,
      email: json['email']! as String,
    );
  }

  final int leadId;
  final String email;

  Map<String, Object?> toJson() => {'leadId': leadId, 'email': email};
}

final class LeadScore {
  const LeadScore({required this.leadId, required this.score});

  factory LeadScore.fromJson(Map<String, Object?> json) {
    return LeadScore(
      leadId: json['leadId']! as int,
      score: json['score']! as int,
    );
  }

  final int leadId;
  final int score;

  Map<String, Object?> toJson() => {'leadId': leadId, 'score': score};
}
