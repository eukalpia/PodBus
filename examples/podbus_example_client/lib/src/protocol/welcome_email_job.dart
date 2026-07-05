/* AUTOMATICALLY GENERATED CODE DO NOT MODIFY */
/*   To generate run: "serverpod generate"    */

// ignore_for_file: implementation_imports
// ignore_for_file: library_private_types_in_public_api
// ignore_for_file: non_constant_identifier_names
// ignore_for_file: public_member_api_docs
// ignore_for_file: type_literal_in_constant_pattern
// ignore_for_file: use_super_parameters
// ignore_for_file: invalid_use_of_internal_member

// ignore_for_file: no_leading_underscores_for_library_prefixes
import 'package:serverpod_client/serverpod_client.dart' as _i1;

abstract class WelcomeEmailJob implements _i1.SerializableModel {
  WelcomeEmailJob._({required this.leadId, required this.email});

  factory WelcomeEmailJob({required int leadId, required String email}) =
      _WelcomeEmailJobImpl;

  factory WelcomeEmailJob.fromJson(Map<String, dynamic> jsonSerialization) {
    return WelcomeEmailJob(
      leadId: jsonSerialization['leadId'] as int,
      email: jsonSerialization['email'] as String,
    );
  }

  int leadId;

  String email;

  /// Returns a shallow copy of this [WelcomeEmailJob]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  WelcomeEmailJob copyWith({int? leadId, String? email});
  @override
  Map<String, dynamic> toJson() {
    return {
      '__className__': 'WelcomeEmailJob',
      'leadId': leadId,
      'email': email,
    };
  }

  @override
  String toString() {
    return _i1.SerializationManager.encode(this);
  }
}

class _WelcomeEmailJobImpl extends WelcomeEmailJob {
  _WelcomeEmailJobImpl({required int leadId, required String email})
    : super._(leadId: leadId, email: email);

  /// Returns a shallow copy of this [WelcomeEmailJob]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  @override
  WelcomeEmailJob copyWith({int? leadId, String? email}) {
    return WelcomeEmailJob(
      leadId: leadId ?? this.leadId,
      email: email ?? this.email,
    );
  }
}
