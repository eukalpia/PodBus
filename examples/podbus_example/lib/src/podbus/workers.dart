import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_example/src/generated/protocol.dart';
import 'package:podbus_serverpod/podbus_serverpod.dart';
import 'package:serverpod/serverpod.dart';

Future<void> registerLeadWorkers(ServerpodMessaging<Session> messaging) async {
  await messaging.subscribe<Map<String, Object?>>(
    'lead.score',
    handler: (session, context, payload) async {
      final lead = Lead.fromJson(payload);
      await context.reply(
        LeadScore(leadId: lead.id, score: _scoreLead(lead)).toJson(),
      );
      session.log('Scored lead ${lead.id}.');
    },
  );

  await messaging.subscribe<Map<String, Object?>>(
    'leads.created',
    queueGroup: 'lead-created-workers',
    handler: (session, context, payload) async {
      final lead = Lead.fromJson(payload);
      session.log('Observed lead-created event for lead ${lead.id}.');
      await context.ack();
    },
  );

  await messaging.worker<Map<String, Object?>>(
    'email.welcome',
    durableName: 'welcome-email-worker',
    retryPolicy: RetryPolicy(
      maxAttempts: 3,
      initialDelay: const Duration(seconds: 1),
      maxDelay: const Duration(seconds: 30),
    ),
    deadLetterPolicy: const DeadLetterPolicy(
      enabled: true,
      destination: 'email.dead',
      includeErrorDetails: true,
    ),
    handler: (session, context, payload) async {
      final job = WelcomeEmailJob.fromJson(payload);
      session.log('Sending welcome email to ${job.email}.');
      await context.ack();
    },
  );

  await messaging.worker<Map<String, Object?>>(
    'email.dead',
    durableName: 'dead-letter-worker',
    handler: (session, context, payload) async {
      final job = WelcomeEmailJob.fromJson(payload);
      session.log(
        'Received dead-letter welcome email job for lead ${job.leadId}.',
        level: LogLevel.warning,
      );
      await context.ack();
    },
  );
}

int _scoreLead(Lead lead) {
  return lead.email.endsWith('@example.com') ? 80 : 65;
}
