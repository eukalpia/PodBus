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
import 'package:serverpod/serverpod.dart' as _i1;

abstract class Lead
    implements _i1.SerializableModel, _i1.ProtocolSerialization {
  Lead._({required this.id, required this.email});

  factory Lead({required int id, required String email}) = _LeadImpl;

  factory Lead.fromJson(Map<String, dynamic> jsonSerialization) {
    return Lead(
      id: jsonSerialization['id'] as int,
      email: jsonSerialization['email'] as String,
    );
  }

  int id;

  String email;

  /// Returns a shallow copy of this [Lead]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  Lead copyWith({int? id, String? email});
  @override
  Map<String, dynamic> toJson() {
    return {'__className__': 'Lead', 'id': id, 'email': email};
  }

  @override
  Map<String, dynamic> toJsonForProtocol() {
    return {'__className__': 'Lead', 'id': id, 'email': email};
  }

  @override
  String toString() {
    return _i1.SerializationManager.encode(this);
  }
}

class _LeadImpl extends Lead {
  _LeadImpl({required int id, required String email})
    : super._(id: id, email: email);

  /// Returns a shallow copy of this [Lead]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  @override
  Lead copyWith({int? id, String? email}) {
    return Lead(id: id ?? this.id, email: email ?? this.email);
  }
}
