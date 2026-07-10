import 'dart:convert';
import 'dart:io';

void main(List<String> arguments) {
  final output = _valueAfter(arguments, '--output');
  final documentName = _valueAfter(arguments, '--name') ?? 'PodBus';
  if (output == null || output.trim().isEmpty) {
    stderr.writeln(
      'Usage: dart run tool/generate_sbom.dart --output <path> '
      '[--name <document-name>]',
    );
    exitCode = 64;
    return;
  }

  final graphFile = File('.dart_tool/package_graph.json');
  if (!graphFile.existsSync()) {
    stderr.writeln(
      'Missing .dart_tool/package_graph.json. Run dart pub get first.',
    );
    exitCode = 1;
    return;
  }

  final graph = jsonDecode(graphFile.readAsStringSync());
  if (graph is! Map<String, Object?>) {
    stderr.writeln('Unexpected package graph format.');
    exitCode = 1;
    return;
  }

  final rawPackages = graph['packages'];
  if (rawPackages is! List<Object?>) {
    stderr.writeln('Package graph does not contain a packages list.');
    exitCode = 1;
    return;
  }

  final packages = <Map<String, Object?>>[];
  final relationships = <Map<String, Object?>>[];
  final usedIds = <String>{};

  for (final rawPackage in rawPackages) {
    if (rawPackage is! Map<String, Object?>) {
      continue;
    }
    final name = rawPackage['name']?.toString();
    if (name == null || name.isEmpty) {
      continue;
    }
    final version = rawPackage['version']?.toString() ?? 'NOASSERTION';
    final id = _uniqueSpdxId(name, usedIds);
    packages.add({
      'name': name,
      'SPDXID': id,
      'versionInfo': version,
      'downloadLocation': 'NOASSERTION',
      'filesAnalyzed': false,
      'licenseConcluded': 'NOASSERTION',
      'licenseDeclared': 'NOASSERTION',
      'copyrightText': 'NOASSERTION',
    });
    relationships.add({
      'spdxElementId': 'SPDXRef-DOCUMENT',
      'relationshipType': 'DESCRIBES',
      'relatedSpdxElement': id,
    });
  }

  packages.sort(
    (left, right) =>
        (left['name'] as String).compareTo(right['name'] as String),
  );

  final now = DateTime.now().toUtc();
  final namespaceTimestamp = now.microsecondsSinceEpoch;
  final document = <String, Object?>{
    'spdxVersion': 'SPDX-2.3',
    'dataLicense': 'CC0-1.0',
    'SPDXID': 'SPDXRef-DOCUMENT',
    'name': documentName,
    'documentNamespace':
        'https://github.com/eukalpia/PodBus/sbom/$namespaceTimestamp',
    'creationInfo': {
      'created': now.toIso8601String(),
      'creators': ['Tool: PodBus SPDX generator'],
    },
    'packages': packages,
    'relationships': relationships,
  };

  final outputFile = File(output);
  outputFile.parent.createSync(recursive: true);
  outputFile.writeAsStringSync(
    '${const JsonEncoder.withIndent('  ').convert(document)}\n',
  );
  stdout.writeln(
    'Wrote SPDX SBOM with ${packages.length} packages to $output.',
  );
}

String? _valueAfter(List<String> arguments, String option) {
  final index = arguments.indexOf(option);
  if (index == -1 || index + 1 >= arguments.length) {
    return null;
  }
  return arguments[index + 1];
}

String _uniqueSpdxId(String name, Set<String> usedIds) {
  final normalized = name.replaceAll(RegExp('[^A-Za-z0-9.-]'), '-');
  final base = 'SPDXRef-Package-$normalized';
  var candidate = base;
  var suffix = 2;
  while (!usedIds.add(candidate)) {
    candidate = '$base-$suffix';
    suffix += 1;
  }
  return candidate;
}
