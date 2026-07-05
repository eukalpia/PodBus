// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart' as nats;

import 'config.dart';

abstract interface class NatsClientAdapter {
  bool get isConnected;

  Future<void> connect(NatsMessagingConfig config);

  Future<void> drain();

  Future<void> close();

  Future<void> flush();

  Future<void> publish(
    String subject,
    List<int> bytes, {
    String? replyTo,
    Map<String, String> headers,
  });

  NatsClientSubscription subscribe(String subject, {String? queueGroup});

  Future<NatsClientMessage> request(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    Map<String, String> headers,
  });
}

abstract interface class NatsClientSubscription {
  Stream<NatsClientMessage> get messages;

  Future<void> close();
}

typedef NatsResponseHandler =
    FutureOr<bool> Function(List<int> bytes, {Map<String, String> headers});

final class NatsClientMessage {
  const NatsClientMessage({
    required this.subject,
    required this.bytes,
    this.headers = const {},
    this.replyTo,
    NatsResponseHandler? responseHandler,
  }) : _responseHandler = responseHandler;

  final String subject;
  final List<int> bytes;
  final Map<String, String> headers;
  final String? replyTo;
  final NatsResponseHandler? _responseHandler;

  Future<bool> respond(
    List<int> bytes, {
    Map<String, String> headers = const {},
  }) async {
    final handler = _responseHandler;
    if (handler == null) {
      return false;
    }
    return handler(bytes, headers: headers);
  }
}

final class DartNatsClientAdapter implements NatsClientAdapter {
  DartNatsClientAdapter({nats.Client? client})
    : _client = client ?? nats.Client();

  final nats.Client _client;

  @override
  bool get isConnected => _client.connected;

  @override
  Future<void> close() => _client.close();

  @override
  Future<void> connect(NatsMessagingConfig config) {
    final connectOption = nats.ConnectOption(
      authToken: config.token,
      user: config.username,
      pass: config.password,
      tlsRequired: config.useTls,
      headers: true,
      name: 'podbus',
    );

    return _client.connect(
      config.servers.first,
      servers: config.servers.length > 1
          ? config.servers.skip(1).toList()
          : null,
      connectOption: connectOption,
      timeout: config.connectTimeout.inSeconds,
      retry: true,
      retryCount: 3,
    );
  }

  @override
  Future<void> drain() => _client.drain();

  @override
  Future<void> flush() => _client.flush();

  @override
  Future<void> publish(
    String subject,
    List<int> bytes, {
    String? replyTo,
    Map<String, String> headers = const {},
  }) async {
    await _client.pub(
      subject,
      Uint8List.fromList(bytes),
      replyTo: replyTo,
      header: nats.Header(headers: headers),
    );
  }

  @override
  Future<NatsClientMessage> request(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    Map<String, String> headers = const {},
  }) async {
    final message = await _client.request<Object?>(
      subject,
      Uint8List.fromList(bytes),
      timeout: timeout,
      header: nats.Header(headers: headers),
    );
    return _fromDartNatsMessage(message);
  }

  @override
  NatsClientSubscription subscribe(String subject, {String? queueGroup}) {
    final subscription = _client.sub<Object?>(subject, queueGroup: queueGroup);
    return _DartNatsClientSubscription(_client, subscription);
  }

  NatsClientMessage _fromDartNatsMessage(nats.Message<Object?> message) {
    final subject = message.subject;
    return NatsClientMessage(
      subject: subject ?? '',
      bytes: message.byte,
      headers: Map.unmodifiable(
        message.header?.headers ?? const <String, String>{},
      ),
      replyTo: message.replyTo,
      responseHandler: (bytes, {headers = const {}}) async {
        final replyTo = message.replyTo;
        if (replyTo == null) {
          return false;
        }
        await _client.pub(
          replyTo,
          Uint8List.fromList(bytes),
          header: nats.Header(headers: headers),
        );
        return true;
      },
    );
  }
}

final class _DartNatsClientSubscription implements NatsClientSubscription {
  _DartNatsClientSubscription(this._client, this._subscription);

  final nats.Client _client;
  final nats.Subscription<Object?> _subscription;

  @override
  Stream<NatsClientMessage> get messages {
    return _subscription.stream.map((message) {
      final subject = message.subject;
      return NatsClientMessage(
        subject: subject ?? '',
        bytes: message.byte,
        headers: Map.unmodifiable(
          message.header?.headers ?? const <String, String>{},
        ),
        replyTo: message.replyTo,
        responseHandler: (bytes, {headers = const {}}) async {
          final replyTo = message.replyTo;
          if (replyTo == null) {
            return false;
          }
          await _client.pub(
            replyTo,
            Uint8List.fromList(bytes),
            header: nats.Header(headers: headers),
          );
          return true;
        },
      );
    });
  }

  @override
  Future<void> close() async {
    _client.unSub(_subscription);
    await _subscription.close();
  }
}
