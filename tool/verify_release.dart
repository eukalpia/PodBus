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

void main(List<String> arguments) {
  if (!arguments.contains('--check')) {
    stderr.writeln(
      'Usage: dart run tool/verify_release.dart --check [--tag=vX.Y.Z]',
    );
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
      failures.add(
        '$path must keep publish_to: none until package publishing is enabled '
        'deliberately.',
      );
    }
  }

  if (versions.values.toSet().length > 1) {
    failures.add('Package versions are inconsistent: $versions.');
  }

  final version = versions.values.isEmpty ? null : versions.values.first;
  if (version != null && !_semanticVersion.hasMatch(version)) {
    failures.add(
      'Package version is not valid semantic version syntax: $version.',
    );
  }

  for (final requiredPath in const [
    'README.md',
    'CHANGELOG.md',
    'SECURITY.md',
    'LICENSE',
    'docs/beta-qualification.md',
    'docs/production.md',
    'website/lib/site.ts',
  ]) {
    if (!File(requiredPath).existsSync()) {
      failures.add('Missing required release file $requiredPath.');
    }
  }

  if (version != null) {
    _requireText(
      failures,
      path: 'README.md',
      expected: version,
      description: 'the package version',
    );
    _requireText(
      failures,
      path: 'CHANGELOG.md',
      expected: '## $version',
      description: 'a release heading for $version',
    );
    _requireText(
      failures,
      path: 'website/lib/site.ts',
      expected: "version: '$version'",
      description: 'the website version',
    );

    final tag =
        _requestedTag(arguments) ?? Platform.environment['GITHUB_REF_NAME'];
    if (tag != null && tag.isNotEmpty && tag.startsWith('v')) {
      final expectedTag = 'v$version';
      if (tag != expectedTag) {
        failures.add(
          'Release tag $tag does not match package version $expectedTag.',
        );
      }
    }
  }

  if (failures.isNotEmpty) {
    for (final failure in failures) {
      stderr.writeln('ERROR: $failure');
    }
    exitCode = 1;
    return;
  }

  stdout.writeln('Release metadata is consistent for $version.');
}

final _semanticVersion = RegExp(
  r'^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)'
  r'(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?'
  r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$',
);

void _requireText(
  List<String> failures, {
  required String path,
  required String expected,
  required String description,
}) {
  final file = File(path);
  if (!file.existsSync()) {
    return;
  }
  if (!file.readAsStringSync().contains(expected)) {
    failures.add('$path must contain $description (`$expected`).');
  }
}

String? _requestedTag(List<String> arguments) {
  for (final argument in arguments) {
    if (argument.startsWith('--tag=')) {
      return argument.substring('--tag='.length).trim();
    }
  }
  return null;
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
  final doubleQuoted = first == '"' && last == '"';
  final singleQuoted = first == "'" && last == "'";
  if (doubleQuoted || singleQuoted) {
    return value.substring(1, value.length - 1);
  }
  return value;
}
