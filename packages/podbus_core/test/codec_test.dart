import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('JsonMessageCodec', () {
    test('encodes JSON-safe maps with content type metadata', () async {
      final codec = JsonMessageCodec();

      final encoded = await codec.encode({'leadId': 42, 'email': 'a@b.test'});
      final json =
          jsonDecode(utf8.decode(encoded.bytes)) as Map<String, Object?>;

      expect(encoded.contentType, 'application/json');
      expect(json, {'leadId': 42, 'email': 'a@b.test'});
    });

    test('decodes maps without requiring a model factory', () async {
      final codec = JsonMessageCodec();
      final encoded = EncodedMessage(
        bytes: utf8.encode('{"leadId":42}'),
        contentType: 'application/json',
        schemaVersion: 1,
      );

      final decoded = await codec.decode<Map<String, Object?>>(encoded);

      expect(decoded, {'leadId': 42});
    });

    test('decodes typed payloads through an explicit factory', () async {
      final codec = JsonMessageCodec();
      final encoded = EncodedMessage(
        bytes: utf8.encode('{"leadId":42,"email":"a@b.test"}'),
        contentType: 'application/json',
        schemaVersion: 1,
      );

      final decoded = await codec.decode<LeadPayload>(
        encoded,
        fromJson: LeadPayload.fromJson,
      );

      expect(decoded, LeadPayload(leadId: 42, email: 'a@b.test'));
    });

    test('throws a codec exception for unsupported payloads', () async {
      final codec = JsonMessageCodec();

      await expectLater(
        codec.encode(Object()),
        throwsA(isA<MessageCodecException>()),
      );
    });
  });
}

final class LeadPayload {
  const LeadPayload({required this.leadId, required this.email});

  factory LeadPayload.fromJson(Object? json) {
    final map = json as Map<String, Object?>;
    return LeadPayload(
      leadId: map['leadId']! as int,
      email: map['email']! as String,
    );
  }

  final int leadId;
  final String email;

  @override
  bool operator ==(Object other) {
    return other is LeadPayload &&
        other.leadId == leadId &&
        other.email == email;
  }

  @override
  int get hashCode => Object.hash(leadId, email);
}
