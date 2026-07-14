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

Future<void> main(List<String> arguments) async {
  final publish = arguments.contains('--publish');
  final dryRun = arguments.contains('--dry-run') || !publish;

  if (publish && arguments.contains('--dry-run')) {
    stderr.writeln('Choose either --dry-run or --publish.');
    exitCode = 64;
    return;
  }

  await _run('dart', ['run', 'tool/verify_release.dart', '--check']);
  final version = _readVersion();

  if (publish) {
    final confirmation = Platform.environment['PODBUS_PUBLISH_CONFIRM'];
    if (confirmation != version) {
      stderr.writeln(
        'Refusing to publish. Set PODBUS_PUBLISH_CONFIRM=$version after reviewing every dry-run.',
      );
      exitCode = 64;
      return;
    }
    await _requireCleanGitTree();
  }

  for (final package in _packages) {
    stdout.writeln(
      '\n=== ${publish ? 'Publishing' : 'Checking'} $package $version ===',
    );
    await _run('dart', [
      'pub',
      '-C',
      'packages/$package',
      'publish',
      if (dryRun) '--dry-run',
    ]);
  }
}

String _readVersion() {
  final content = File('packages/podbus_core/pubspec.yaml').readAsStringSync();
  final match = RegExp(
    r'^version:\s*(\S+)\s*$',
    multiLine: true,
  ).firstMatch(content);
  if (match == null) {
    throw StateError('podbus_core version is missing.');
  }
  return match.group(1)!;
}

Future<void> _requireCleanGitTree() async {
  await _run('git', ['diff', '--quiet']);
  await _run('git', ['diff', '--cached', '--quiet']);
}

Future<void> _run(String executable, List<String> arguments) async {
  final process = await Process.start(
    executable,
    arguments,
    mode: ProcessStartMode.inheritStdio,
  );
  final result = await process.exitCode;
  if (result != 0) {
    throw ProcessException(
      executable,
      arguments,
      'Exited with $result',
      result,
    );
  }
}
