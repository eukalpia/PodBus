from pathlib import Path

path = Path('packages/podbus_nats/test/nats_jetstream_adapter_test.dart')
text = path.read_text()

old_import = "import 'package:dart_nats/dart_nats.dart' as nats;\n"
new_import = old_import + "import 'package:podbus_core/podbus_core.dart';\n"
if old_import not in text:
    raise SystemExit('dart_nats import not found')
if "package:podbus_core/podbus_core.dart" not in text:
    text = text.replace(old_import, new_import, 1)

old = """      final publishExpectation = expectLater(
        pending,
        throwsA(isA<TimeoutException>()),
      );
"""
new = """      final publishExpectation = expectLater(
        pending,
        throwsA(
          isA<MessagingTimeoutException>().having(
            (error) => error.timeout,
            'timeout',
            const Duration(milliseconds: 25),
          ),
        ),
      );
"""
if text.count(old) != 1:
    raise SystemExit(f'publish timeout assertion count={text.count(old)}')
text = text.replace(old, new, 1)
path.write_text(text)
