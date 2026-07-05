import 'headers.dart';

final class MessageEnvelope<T> {
  const MessageEnvelope({
    required this.id,
    required this.subject,
    required this.payload,
    required this.headers,
    required this.createdAt,
    required this.schemaVersion,
    required this.contentType,
  });

  final String id;
  final String subject;
  String get topic => subject;
  final T payload;
  final MessageHeaders headers;
  final DateTime createdAt;
  final int schemaVersion;
  final String contentType;
}
