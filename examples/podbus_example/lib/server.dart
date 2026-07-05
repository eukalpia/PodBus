import 'package:podbus_example/src/generated/endpoints.dart';
import 'package:podbus_example/src/generated/protocol.dart';
import 'package:podbus_example/src/podbus/bootstrap.dart';
import 'package:podbus_example/src/podbus/runtime.dart';
import 'package:serverpod/serverpod.dart';

void run(List<String> args) async {
  final pod = Serverpod(args, Protocol(), Endpoints());
  final module = await createPodBusModule();
  PodBusRuntime.configure(module.messaging);

  await module.start();
  pod.experimental.shutdownTasks.addTask('podbus', () async {
    await module.stop(timeout: const Duration(seconds: 10));
  });

  await pod.start();
}
