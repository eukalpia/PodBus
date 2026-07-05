import 'package:podbus_serverpod/podbus_serverpod.dart';
import 'package:serverpod/serverpod.dart';

final class PodBusRuntime {
  static ServerpodMessaging<Session>? _messaging;

  static ServerpodMessaging<Session> get messaging {
    final messaging = _messaging;
    if (messaging == null) {
      throw StateError('PodBus runtime has not been configured.');
    }
    return messaging;
  }

  static void configure(ServerpodMessaging<Session> messaging) {
    _messaging = messaging;
  }
}
