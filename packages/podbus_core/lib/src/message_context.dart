import 'headers.dart';

abstract interface class MessageContext {
  String get subject;

  MessageHeaders get headers;

  Object? get rawMessage;

  Future<void> ack();

  Future<void> nak({Duration? delay});

  Future<void> terminate();

  Future<void> extendVisibility(Duration duration);

  Future<void> reply<T>(T payload, {MessageHeaders? headers});
}

abstract interface class JobContext {
  String get topic;

  MessageHeaders get headers;

  int get attempt;

  int get maxAttempts;

  Object? get rawMessage;

  Future<void> ack();

  Future<void> retry({Duration? delay});

  Future<void> deadLetter({Object? error, StackTrace? stackTrace});

  Future<void> fail(Object error, [StackTrace? stackTrace]);
}
