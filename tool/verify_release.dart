import 'dart:io';

const _packages = <String>[
  'podbus_core',
  'podbus_nats',
  'podbus_rabbitmq',
  'podbus_kafka',
  'podbus_serverpod',
];

const _repository = 'https://github.com/eukalpia/PodBus';

void main(List<String> arguments) {
  if (!arguments.contains('--check')) {
    stderr.writeln('Usage: dart run tool/verify_release.dart --check');
    exitCode = 64;
    return;
  }

  final failures = <String>[];
  final versions = <String, String>{};

  for (final package in _packages) {
    final path = 'packages/$package/pubspec.yaml';
    final file = File(path);
    if (!file.existsSync()) {
      failures.add('Missing $path.');
      continue;
    }

    final fields = _topLevelFields(file.readAsLinesSync());
    final version = fields['version'];
    final repository = fields['repository'];
    final publishTo = fields['publish_to'];

    if (version == null || version.isEmpty) {
      failures.add('$path does not declare a version.');
    } else {
      versions[package] = version;
    }
    if (repository != _repository) {
      failures.add('$path repository must be $_repository.');
    }
    if (publishTo != 'none') {
      failures.add('$path must keep publish_to: none until package publishing is enabled deliberately.');
    }
  }

  if (versions.values.toSet().length > 1) {
    failures.add('Package versions are inconsistent: $versions.');
  }

  for (final requiredPath in const [
    'README.md',
    'CHANGELOG.md',
    'SECURITY.md',
    'LICENSE',
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

  stdout.writeln('Release metadata is consistent for ${versions.values.single}.');
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
    result[key] = value.replaceAll(RegExp(r'^["\']|["\']$'), '');
  }
  return result;
}
