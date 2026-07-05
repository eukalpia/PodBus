import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('InMemoryMessageBus', () {
    test('delivers published messages to matching subscribers', () async {
      final bus = InMemoryMessageBus();
      await bus.connect();

      final received = Completer<LeadCreated>();
      await bus.subscribe<LeadCreated>(
        'leads.created',
        handler: (context, payload) async {
          expect(context.subject, 'leads.created');
          expect(context.headers.correlationId, 'corr-1');
          received.complete(payload);
        },
      );

      await bus.publish(
        'leads.created',
        LeadCreated(leadId: 7),
        headers: MessageHeaders(correlationId: 'corr-1'),
      );

      expect(
        await received.future.timeout(Duration(seconds: 1)),
        LeadCreated(leadId: 7),
      );
      await bus.close();
    });

    test(
      'load balances messages across subscribers in the same queue group',
      () async {
        final bus = InMemoryMessageBus();
        await bus.connect();

        var first = 0;
        var second = 0;

        await bus.subscribe<int>(
          'jobs.email',
          queueGroup: 'email-workers',
          handler: (_, _) async => first++,
        );
        await bus.subscribe<int>(
          'jobs.email',
          queueGroup: 'email-workers',
          handler: (_, _) async => second++,
        );

        await bus.publish('jobs.email', 1);
        await bus.publish('jobs.email', 2);
        await Future<void>.delayed(Duration(milliseconds: 10));

        expect(first + second, 2);
        expect(first, 1);
        expect(second, 1);
        await bus.close();
      },
    );

    test('supports request reply through MessageContext.reply', () async {
      final bus = InMemoryMessageBus();
      await bus.connect();

      await bus.subscribe<LeadScoreRequest>(
        'lead.score',
        handler: (context, payload) async {
          await context.reply(
            LeadScoreResponse(leadId: payload.leadId, score: 91),
          );
        },
      );

      final response = await bus.request<LeadScoreRequest, LeadScoreResponse>(
        'lead.score',
        LeadScoreRequest(leadId: 7),
        timeout: Duration(seconds: 1),
      );

      expect(response, LeadScoreResponse(leadId: 7, score: 91));
      await bus.close();
    });

    test(
      'reports unhealthy before connect and healthy after connect',
      () async {
        final bus = InMemoryMessageBus();

        expect((await bus.healthCheck()).status, HealthStatus.unhealthy);

        await bus.connect();

        expect((await bus.healthCheck()).status, HealthStatus.healthy);
        await bus.close();
      },
    );
  });
}

final class LeadCreated {
  const LeadCreated({required this.leadId});

  final int leadId;

  @override
  bool operator ==(Object other) {
    return other is LeadCreated && other.leadId == leadId;
  }

  @override
  int get hashCode => leadId.hashCode;
}

final class LeadScoreRequest {
  const LeadScoreRequest({required this.leadId});

  final int leadId;
}

final class LeadScoreResponse {
  const LeadScoreResponse({required this.leadId, required this.score});

  final int leadId;
  final int score;

  @override
  bool operator ==(Object other) {
    return other is LeadScoreResponse &&
        other.leadId == leadId &&
        other.score == score;
  }

  @override
  int get hashCode => Object.hash(leadId, score);
}
