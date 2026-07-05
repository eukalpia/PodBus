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

abstract class LeadScore implements _i1.SerializableModel {
  LeadScore._({required this.leadId, required this.score});

  factory LeadScore({required int leadId, required int score}) = _LeadScoreImpl;

  factory LeadScore.fromJson(Map<String, dynamic> jsonSerialization) {
    return LeadScore(
      leadId: jsonSerialization['leadId'] as int,
      score: jsonSerialization['score'] as int,
    );
  }

  int leadId;

  int score;

  /// Returns a shallow copy of this [LeadScore]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  LeadScore copyWith({int? leadId, int? score});
  @override
  Map<String, dynamic> toJson() {
    return {'__className__': 'LeadScore', 'leadId': leadId, 'score': score};
  }

  @override
  String toString() {
    return _i1.SerializationManager.encode(this);
  }
}

class _LeadScoreImpl extends LeadScore {
  _LeadScoreImpl({required int leadId, required int score})
    : super._(leadId: leadId, score: score);

  /// Returns a shallow copy of this [LeadScore]
  /// with some or all fields replaced by the given arguments.
  @_i1.useResult
  @override
  LeadScore copyWith({int? leadId, int? score}) {
    return LeadScore(leadId: leadId ?? this.leadId, score: score ?? this.score);
  }
}
