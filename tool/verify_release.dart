import 'dart:io';

const _packages = <String>[
  'podbus_core',
  'podbus_nats',
  'podbus_rabbitmq',
  'podbus_kafka',
  'podbus_postgres',
  'podbus_observability',
  'podbus_serverpod',
];

const _repository = 'https://github.com/eukalpia/PodBus';
const _issueTracker = 'https://github.com/eukalpia/PodBus/issues';

void main(List<String> arguments) {
  if (!arguments.contains('--check')) {
    stderr.writeln('Usage: dart run tool/verify_release.dart --check');
    exitCode = 64;
    return;
  }

  final failures = <String>[];
  final versions = <String, String>{};

  for (final package in _packages) {
    final packageDirectory = Directory('packages/$package');
    final pubspec = File('${packageDirectory.path}/pubspec.yaml');
    if (!pubspec.existsSync()) {
      failures.add('Missing ${pubspec.path}.');
      continue;
    }

    final content = pubspec.readAsStringSync();
    final fields = _topLevelFields(content.split('\n'));
    final version = fields['version'];

    if (version == null || version.isEmpty) {
      failures.add('${pubspec.path} does not declare a version.');
    } else {
      versions[package] = version;
    }
    if (fields['repository'] != _repository) {
      failures.add('${pubspec.path} repository must be $_repository.');
    }
    if (fields['issue_tracker'] != _issueTracker) {
      failures.add('${pubspec.path} issue_tracker must be $_issueTracker.');
    }
    if (fields.containsKey('publish_to')) {
      failures.add(
        '${pubspec.path} must use the default pub.dev publisher; remove publish_to.',
      );
    }
    if (content.contains(
      RegExp(r'^\s+path:\s+\.\./podbus_', multiLine: true),
    )) {
      failures.add('${pubspec.path} contains a local PodBus path dependency.');
    }

    for (final filename in const ['README.md', 'CHANGELOG.md', 'LICENSE']) {
      final file = File('${packageDirectory.path}/$filename');
      if (!file.existsSync() || file.lengthSync() == 0) {
        failures.add('Missing non-empty ${file.path}.');
      }
    }
  }

  if (versions.values.toSet().length > 1) {
    failures.add('Package versions are inconsistent: $versions.');
  }

  final version = versions.values.isEmpty ? null : versions.values.first;
  if (version != null) {
    if (!version.contains('-beta.')) {
      failures.add('Expected a beta version, found $version.');
    }
    for (final package in _packages.where((name) => name != 'podbus_core')) {
      final content = File('packages/$package/pubspec.yaml').readAsStringSync();
      if (!content.contains('podbus_core: ^$version')) {
        failures.add(
          'packages/$package/pubspec.yaml must depend on podbus_core: ^$version.',
        );
      }
    }
    for (final package in _packages) {
      final changelog = File(
        'packages/$package/CHANGELOG.md',
      ).readAsStringSync();
      if (!changelog.contains(version)) {
        failures.add(
          'packages/$package/CHANGELOG.md does not mention $version.',
        );
      }
    }
    final readme = File('README.md').readAsStringSync();
    if (!readme.contains(version) || !readme.contains('status-beta')) {
      failures.add('README.md must advertise $version and beta status.');
    }
    final changelog = File('CHANGELOG.md').readAsStringSync();
    if (!changelog.contains('## $version')) {
      failures.add('CHANGELOG.md does not contain a $version release section.');
    }
  }

  for (final requiredPath in const [
    'README.md',
    'CHANGELOG.md',
    'SECURITY.md',
    'LICENSE',
    'tool/publish_packages.dart',
  ]) {
    if (!File(requiredPath).existsSync()) {
      failures.add('Missing required release file $requiredPath.');
    }
  }

  if (failures.isNotEmpty) {
    for (final failure in failures) {
      stderr.writeln('ERROR: $failure');
    }
    exitCode = 1;
    return;
  }

  stdout.writeln('Release metadata is publish-ready for $version.');
}

Map<String, String> _topLevelFields(List<String> lines) {
  final result = <String, String>{};
  for (final line in lines) {
    if (line.isEmpty || line.startsWith(' ') || line.startsWith('#')) {
      continue;
    }
    final separator = line.indexOf(':');
    if (separator <= 0) {
      continue;
    }
    final key = line.substring(0, separator).trim();
    final value = line.substring(separator + 1).trim();
    result[key] = _unquote(value);
  }
  return result;
}

String _unquote(String value) {
  if (value.length < 2) {
    return value;
  }
  final first = value[0];
  final last = value[value.length - 1];
  if ((first == '"' && last == '"') || (first == "'" && last == "'")) {
    return value.substring(1, value.length - 1);
  }
  return value;
}
