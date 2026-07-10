import 'dart:convert';
import 'dart:io';

void main(List<String> arguments) {
  final directory = Directory(_value(arguments, '--directory') ?? 'coverage');
  final minimum = double.tryParse(_value(arguments, '--minimum') ?? '45');
  if (minimum == null || minimum < 0 || minimum > 100) {
    stderr.writeln('Coverage minimum must be between 0 and 100.');
    exitCode = 64;
    return;
  }
  if (!directory.existsSync()) {
    stderr.writeln('Coverage directory ${directory.path} does not exist.');
    exitCode = 1;
    return;
  }

  final hitsBySource = <String, Map<int, int>>{};
  for (final entity in directory.listSync(recursive: true)) {
    if (entity is! File || !entity.path.endsWith('.vm.json')) {
      continue;
    }
    final decoded = jsonDecode(entity.readAsStringSync());
    if (decoded is! Map<String, Object?>) {
      continue;
    }
    final coverage = decoded['coverage'];
    if (coverage is! List<Object?>) {
      continue;
    }
    for (final rawEntry in coverage) {
      if (rawEntry is! Map<String, Object?>) {
        continue;
      }
      final source = rawEntry['source'];
      final rawHits = rawEntry['hits'];
      if (source is! String || rawHits is! List<Object?>) {
        continue;
      }
      if (!source.startsWith('package:podbus_') || source.contains('/test/')) {
        continue;
      }
      final sourceHits = hitsBySource.putIfAbsent(source, () => <int, int>{});
      for (var index = 0; index + 1 < rawHits.length; index += 2) {
        final line = rawHits[index];
        final count = rawHits[index + 1];
        if (line is! int || count is! int) {
          continue;
        }
        final existing = sourceHits[line] ?? 0;
        if (count > existing) {
          sourceHits[line] = count;
        }
      }
    }
  }

  if (hitsBySource.isEmpty) {
    stderr.writeln(
      'No PodBus library coverage was found in ${directory.path}.',
    );
    exitCode = 1;
    return;
  }

  final packages = <String, _Coverage>{};
  var covered = 0;
  var total = 0;
  for (final entry in hitsBySource.entries) {
    final package = entry.key.substring('package:'.length).split('/').first;
    final packageCoverage = packages.putIfAbsent(package, _Coverage.new);
    for (final count in entry.value.values) {
      total += 1;
      packageCoverage.total += 1;
      if (count > 0) {
        covered += 1;
        packageCoverage.covered += 1;
      }
    }
  }

  final percentage = total == 0 ? 0.0 : covered * 100 / total;
  stdout.writeln(
    'PodBus coverage: ${percentage.toStringAsFixed(2)}% '
    '($covered/$total executable lines)',
  );
  for (final entry
      in packages.entries.toList()
        ..sort((left, right) => left.key.compareTo(right.key))) {
    stdout.writeln(
      '  ${entry.key}: ${entry.value.percentage.toStringAsFixed(2)}% '
      '(${entry.value.covered}/${entry.value.total})',
    );
  }

  if (percentage < minimum) {
    stderr.writeln(
      'Coverage ${percentage.toStringAsFixed(2)}% is below the required '
      '${minimum.toStringAsFixed(2)}%.',
    );
    exitCode = 1;
  }
}

String? _value(List<String> arguments, String option) {
  final index = arguments.indexOf(option);
  if (index < 0 || index + 1 >= arguments.length) {
    return null;
  }
  return arguments[index + 1];
}

final class _Coverage {
  int covered = 0;
  int total = 0;

  double get percentage => total == 0 ? 0 : covered * 100 / total;
}
