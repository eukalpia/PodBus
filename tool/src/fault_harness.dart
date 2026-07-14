import 'dart:async';
import 'dart:convert';
import 'dart:io';

final class FaultEnvironment {
  FaultEnvironment({
    this.composeFile = 'docker-compose.integration.yaml',
    Uri? toxiproxyApi,
  }) : toxiproxy = ToxiproxyClient(
         toxiproxyApi ?? Uri.parse('http://127.0.0.1:8474'),
       );

  final String composeFile;
  final ToxiproxyClient toxiproxy;

  Future<void> startServices(Iterable<String> services) async {
    await dockerCompose(['up', '-d', ...services]);
  }

  Future<void> stopServices({bool removeVolumes = false}) async {
    await dockerCompose(['down', if (removeVolumes) '-v']);
  }

  Future<void> stopService(String service) async {
    await dockerCompose(['stop', service]);
  }

  Future<void> startService(String service) async {
    await dockerCompose(['start', service]);
  }

  Future<void> restartService(String service) async {
    await dockerCompose(['restart', service]);
  }

  Future<ProcessResult> dockerCompose(
    List<String> arguments, {
    bool allowFailure = false,
  }) async {
    final result = await Process.run('docker', [
      'compose',
      '-f',
      composeFile,
      ...arguments,
    ]);
    if (!allowFailure && result.exitCode != 0) {
      throw ProcessException(
        'docker',
        ['compose', '-f', composeFile, ...arguments],
        '${result.stdout}\n${result.stderr}',
        result.exitCode,
      );
    }
    return result;
  }

  Future<void> waitForPort(
    String host,
    int port, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    await waitUntil(
      () async {
        Socket? socket;
        try {
          socket = await Socket.connect(
            host,
            port,
            timeout: const Duration(milliseconds: 500),
          );
          return true;
        } on Object {
          return false;
        } finally {
          socket?.destroy();
        }
      },
      timeout: timeout,
      description: '$host:$port to accept TCP connections',
    );
  }

  Future<void> waitForService(String service) async {
    switch (service) {
      case 'nats':
        await waitForPort('127.0.0.1', 4222);
      case 'rabbitmq':
        await waitForPort('127.0.0.1', 5672);
      case 'toxiproxy':
        await waitUntil(
          toxiproxy.isReady,
          timeout: const Duration(seconds: 30),
          description: 'Toxiproxy API readiness',
        );
      default:
        throw ArgumentError.value(service, 'service', 'Unsupported service');
    }
  }
}

final class ToxiproxyClient {
  ToxiproxyClient(this.baseUri);

  final Uri baseUri;

  Future<bool> isReady() async {
    try {
      final response = await _request('GET', '/version');
      return response.statusCode >= 200 && response.statusCode < 300;
    } on Object {
      return false;
    }
  }

  Future<void> reset() async {
    final response = await _request('POST', '/reset');
    _expectSuccess(response, 'reset Toxiproxy');
  }

  Future<void> deleteProxy(String name) async {
    final response = await _request(
      'DELETE',
      '/proxies/${Uri.encodeComponent(name)}',
    );
    if (response.statusCode != HttpStatus.notFound) {
      _expectSuccess(response, 'delete proxy $name');
    }
  }

  Future<void> createProxy({
    required String name,
    required String listen,
    required String upstream,
  }) async {
    await deleteProxy(name);
    final response = await _request(
      'POST',
      '/proxies',
      body: {
        'name': name,
        'listen': listen,
        'upstream': upstream,
        'enabled': true,
      },
    );
    _expectSuccess(response, 'create proxy $name');
  }

  Future<void> setEnabled(String name, bool enabled) async {
    final current = await _request(
      'GET',
      '/proxies/${Uri.encodeComponent(name)}',
    );
    _expectSuccess(current, 'read proxy $name');
    final document = jsonDecode(current.body) as Map<String, dynamic>;
    final response = await _request(
      'POST',
      '/proxies/${Uri.encodeComponent(name)}',
      body: {
        'name': document['name'],
        'listen': document['listen'],
        'upstream': document['upstream'],
        'enabled': enabled,
      },
    );
    _expectSuccess(response, '${enabled ? 'enable' : 'disable'} proxy $name');
  }

  Future<void> addLatency({
    required String proxy,
    required String toxic,
    required String stream,
    required Duration latency,
    Duration jitter = Duration.zero,
  }) async {
    await removeToxic(proxy: proxy, toxic: toxic);
    final response = await _request(
      'POST',
      '/proxies/${Uri.encodeComponent(proxy)}/toxics',
      body: {
        'name': toxic,
        'type': 'latency',
        'stream': stream,
        'toxicity': 1.0,
        'attributes': {
          'latency': latency.inMilliseconds,
          'jitter': jitter.inMilliseconds,
        },
      },
    );
    _expectSuccess(response, 'add latency toxic $toxic');
  }

  Future<void> addTimeout({
    required String proxy,
    required String toxic,
    required String stream,
    required Duration timeout,
  }) async {
    await removeToxic(proxy: proxy, toxic: toxic);
    final response = await _request(
      'POST',
      '/proxies/${Uri.encodeComponent(proxy)}/toxics',
      body: {
        'name': toxic,
        'type': 'timeout',
        'stream': stream,
        'toxicity': 1.0,
        'attributes': {'timeout': timeout.inMilliseconds},
      },
    );
    _expectSuccess(response, 'add timeout toxic $toxic');
  }

  Future<void> addResetPeer({
    required String proxy,
    required String toxic,
    required String stream,
    Duration timeout = Duration.zero,
  }) async {
    await removeToxic(proxy: proxy, toxic: toxic);
    final response = await _request(
      'POST',
      '/proxies/${Uri.encodeComponent(proxy)}/toxics',
      body: {
        'name': toxic,
        'type': 'reset_peer',
        'stream': stream,
        'toxicity': 1.0,
        'attributes': {'timeout': timeout.inMilliseconds},
      },
    );
    _expectSuccess(response, 'add reset_peer toxic $toxic');
  }

  Future<void> removeToxic({
    required String proxy,
    required String toxic,
  }) async {
    final response = await _request(
      'DELETE',
      '/proxies/${Uri.encodeComponent(proxy)}/toxics/'
          '${Uri.encodeComponent(toxic)}',
    );
    if (response.statusCode != HttpStatus.notFound) {
      _expectSuccess(response, 'remove toxic $toxic');
    }
  }

  Future<_HttpResult> _request(
    String method,
    String path, {
    Map<String, Object?>? body,
  }) async {
    Socket? socket;
    try {
      final uri = baseUri.resolve(path);
      final port = uri.hasPort
          ? uri.port
          : uri.scheme == 'https'
          ? 443
          : 80;
      if (uri.scheme != 'http') {
        throw ArgumentError.value(
          uri,
          'baseUri',
          'Fault-control HTTP requires a local http endpoint.',
        );
      }
      socket = await Socket.connect(
        uri.host,
        port,
        timeout: const Duration(seconds: 3),
      );
      socket.setOption(SocketOption.tcpNoDelay, true);

      final payload = body == null
          ? const <int>[]
          : utf8.encode(jsonEncode(body));
      final target = uri.hasQuery ? '${uri.path}?${uri.query}' : uri.path;
      final request = StringBuffer()
        ..write('$method ${target.isEmpty ? '/' : target} HTTP/1.1\r\n')
        ..write('Host: ${uri.host}:$port\r\n')
        ..write('Accept: application/json\r\n')
        ..write('Connection: close\r\n')
        ..write('Content-Type: application/json\r\n')
        ..write('Content-Length: ${payload.length}\r\n\r\n');
      socket.add(utf8.encode(request.toString()));
      if (payload.isNotEmpty) {
        socket.add(payload);
      }
      await socket.flush();

      final bytes = <int>[];
      await for (final chunk in socket.timeout(const Duration(seconds: 5))) {
        bytes.addAll(chunk);
      }
      return _parseHttpResponse(bytes);
    } finally {
      socket?.destroy();
    }
  }

  _HttpResult _parseHttpResponse(List<int> bytes) {
    final raw = latin1.decode(bytes);
    final separator = raw.indexOf('\r\n\r\n');
    if (separator < 0) {
      throw FormatException('Malformed HTTP response from Toxiproxy.');
    }
    final headerText = raw.substring(0, separator);
    final headerLines = headerText.split('\r\n');
    final statusParts = headerLines.first.split(' ');
    if (statusParts.length < 2) {
      throw FormatException('Malformed HTTP status line: ${headerLines.first}');
    }
    final statusCode = int.parse(statusParts[1]);
    final headers = <String, String>{};
    for (final line in headerLines.skip(1)) {
      final colon = line.indexOf(':');
      if (colon > 0) {
        headers[line.substring(0, colon).trim().toLowerCase()] = line
            .substring(colon + 1)
            .trim();
      }
    }
    final bodyBytes = bytes.sublist(separator + 4);
    final responseBody =
        headers['transfer-encoding']?.toLowerCase() == 'chunked'
        ? utf8.decode(_decodeChunked(bodyBytes))
        : utf8.decode(bodyBytes);
    return _HttpResult(statusCode, responseBody);
  }

  List<int> _decodeChunked(List<int> bytes) {
    final result = <int>[];
    var offset = 0;
    while (offset < bytes.length) {
      final lineEnd = _indexOfCrlf(bytes, offset);
      if (lineEnd < 0) {
        throw const FormatException('Malformed chunked HTTP response.');
      }
      final sizeText = ascii
          .decode(bytes.sublist(offset, lineEnd))
          .split(';')
          .first;
      final size = int.parse(sizeText.trim(), radix: 16);
      offset = lineEnd + 2;
      if (size == 0) {
        return result;
      }
      if (offset + size + 2 > bytes.length) {
        throw const FormatException('Truncated chunked HTTP response.');
      }
      result.addAll(bytes.sublist(offset, offset + size));
      offset += size;
      if (bytes[offset] != 13 || bytes[offset + 1] != 10) {
        throw const FormatException('Malformed chunk delimiter.');
      }
      offset += 2;
    }
    throw const FormatException('Missing final HTTP chunk.');
  }

  int _indexOfCrlf(List<int> bytes, int start) {
    for (var index = start; index + 1 < bytes.length; index += 1) {
      if (bytes[index] == 13 && bytes[index + 1] == 10) {
        return index;
      }
    }
    return -1;
  }

  void _expectSuccess(_HttpResult response, String action) {
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw StateError(
        'Failed to $action: HTTP ${response.statusCode}: ${response.body}',
      );
    }
  }
}

final class _HttpResult {
  const _HttpResult(this.statusCode, this.body);

  final int statusCode;
  final String body;
}

final class FaultScenarioResult {
  FaultScenarioResult({
    required this.name,
    required this.startedAt,
    required this.finishedAt,
    required this.metrics,
  });

  final String name;
  final DateTime startedAt;
  final DateTime finishedAt;
  final Map<String, Object?> metrics;

  Duration get duration => finishedAt.difference(startedAt);

  Map<String, Object?> toJson() => {
    'name': name,
    'startedAt': startedAt.toIso8601String(),
    'finishedAt': finishedAt.toIso8601String(),
    'durationMs': duration.inMilliseconds,
    'metrics': metrics,
  };
}

Future<void> waitUntil(
  FutureOr<bool> Function() predicate, {
  required Duration timeout,
  required String description,
  Duration pollInterval = const Duration(milliseconds: 50),
}) async {
  final deadline = DateTime.now().add(timeout);
  Object? lastError;
  while (DateTime.now().isBefore(deadline)) {
    try {
      if (await predicate()) {
        return;
      }
    } on Object catch (error) {
      lastError = error;
    }
    await Future<void>.delayed(pollInterval);
  }
  throw TimeoutException(
    'Timed out waiting for $description'
    '${lastError == null ? '' : ': $lastError'}',
    timeout,
  );
}

Duration parseDuration(String value) {
  final match = RegExp(r'^(\d+)(ms|s|m|h)$').firstMatch(value.trim());
  if (match == null) {
    throw FormatException('Invalid duration "$value". Use ms, s, m, or h.');
  }
  final amount = int.parse(match.group(1)!);
  return switch (match.group(2)) {
    'ms' => Duration(milliseconds: amount),
    's' => Duration(seconds: amount),
    'm' => Duration(minutes: amount),
    'h' => Duration(hours: amount),
    _ => throw StateError('Unreachable duration unit.'),
  };
}

Future<void> appendJsonLine(File file, Map<String, Object?> value) async {
  await file.parent.create(recursive: true);
  await file.writeAsString(
    '${jsonEncode(value)}\n',
    mode: FileMode.append,
    flush: true,
  );
}

List<Map<String, dynamic>> readJsonLines(File file) {
  if (!file.existsSync()) {
    return const [];
  }
  return [
    for (final line in file.readAsLinesSync())
      if (line.trim().isNotEmpty) jsonDecode(line) as Map<String, dynamic>,
  ];
}
