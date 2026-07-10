#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVERPOD = ROOT / "serverpod"

restrictions_path = (
    SERVERPOD
    / "tools/serverpod_cli/lib/src/analyzer/models/validation/restrictions.dart"
)
tests_path = (
    SERVERPOD
    / "tools/serverpod_cli/test/analyzer/models/stateful_analyzer/"
    "model_validation/relation/relation_database_action_test.dart"
)
changelog_path = SERVERPOD / "tools/serverpod_cli/CHANGELOG.md"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


restrictions = restrictions_path.read_text()
restrictions = replace_once(
    restrictions,
    '  List<SourceSpanSeverityException> validateDatabaseActionKey(\n    String parentNodeName,\n    String key,\n    SourceSpan? span,\n  ) {\n    var definition = documentDefinition;\n    if (definition is! ClassDefinition) return [];\n\n    var field = definition.findField(parentNodeName);\n\n    if (field?.relation?.isForeignKeyOrigin == false) {\n      return [\n        SourceSpanSeverityException(\n          \'The "$key" property can only be set on the side holding the foreign key.\',\n          span,\n        ),\n      ];\n    }\n\n    return [];\n  }\n',
    '  List<SourceSpanSeverityException> validateDatabaseActionKey(\n    String parentNodeName,\n    String key,\n    SourceSpan? span,\n  ) {\n    var definition = documentDefinition;\n    if (definition is! ClassDefinition) return [];\n\n    var relationField = definition.findField(parentNodeName);\n    var relation = relationField?.relation;\n\n    if (relation?.isForeignKeyOrigin == false) {\n      return [\n        SourceSpanSeverityException(\n          \'The "$key" property can only be set on the side holding the foreign key.\',\n          span,\n        ),\n      ];\n    }\n\n    var foreignKeyField = _resolveForeignKeyField(\n      definition,\n      relationField,\n    );\n    var foreignKeyRelation = foreignKeyField?.relation;\n    if (foreignKeyRelation is! ForeignRelationDefinition) return [];\n\n    var action = switch (key) {\n      Keyword.onDelete => foreignKeyRelation.onDelete,\n      Keyword.onUpdate => foreignKeyRelation.onUpdate,\n      _ => null,\n    };\n\n    if (action == ForeignKeyAction.setNull) {\n      return _validateSetNullAction(key, foreignKeyField!, span);\n    }\n\n    if (action == ForeignKeyAction.setDefault) {\n      return _validateSetDefaultAction(key, foreignKeyField!, span);\n    }\n\n    return [];\n  }\n\n  SerializableModelFieldDefinition? _resolveForeignKeyField(\n    ClassDefinition definition,\n    SerializableModelFieldDefinition? relationField,\n  ) {\n    var relation = relationField?.relation;\n\n    if (relation is ForeignRelationDefinition) {\n      return relationField;\n    }\n\n    if (relation is ObjectRelationDefinition) {\n      return definition.findField(relation.fieldName);\n    }\n\n    return null;\n  }\n\n  List<SourceSpanSeverityException> _validateSetNullAction(\n    String key,\n    SerializableModelFieldDefinition foreignKeyField,\n    SourceSpan? span,\n  ) {\n    var isNullableDatabaseColumn =\n        foreignKeyField.name != defaultPrimaryKeyName &&\n        foreignKeyField.type.nullable;\n    if (isNullableDatabaseColumn) return [];\n\n    return [\n      SourceSpanSeverityException(\n        \'The foreign key field "${foreignKeyField.name}" must be nullable in \'\n        \'the database when "$key" is set to "SetNull".\',\n        span,\n      ),\n    ];\n  }\n\n  List<SourceSpanSeverityException> _validateSetDefaultAction(\n    String key,\n    SerializableModelFieldDefinition foreignKeyField,\n    SourceSpan? span,\n  ) {\n    if (foreignKeyField.defaultPersistValue != null) return [];\n\n    return [\n      SourceSpanSeverityException(\n        \'The foreign key field "${foreignKeyField.name}" must define a \'\n        \'database default using "default" or "defaultPersist" when "$key" is \'\n        \'set to "SetDefault". "defaultModel" only initializes Dart objects \'\n        \'and is not applied by the database.\',\n        span,\n      ),\n    ];\n  }\n',
    "database action validator",
)
restrictions_path.write_text(restrictions)

tests = tests_path.read_text()
tests = replace_once(
    tests,
    'void main() {\n  var config = GeneratorConfigBuilder().build();\n',
    'void main() {\n  var config = GeneratorConfigBuilder().build();\n\n  CodeGenerationCollector analyzeSingleModel(String yaml) {\n    var collector = CodeGenerationCollector();\n    StatefulAnalyzer(\n      config,\n      [ModelSourceBuilder().withYaml(yaml).build()],\n      onErrorsCollector(collector),\n    ).validateAll();\n    return collector;\n  }\n',
    "test helper",
)
tests = replace_once(
    tests,
    '        fields:\n          example: Example?, relation(onUpdate=$action)\n',
    '        fields:\n          exampleId: int?, default=1\n          example: Example?, relation(field=exampleId, onUpdate=$action)\n',
    "valid onUpdate fixture",
)
tests = replace_once(
    tests,
    '        fields:\n          example: Example?, relation(onDelete=$action)\n',
    '        fields:\n          exampleId: int?, default=1\n          example: Example?, relation(field=exampleId, onDelete=$action)\n',
    "valid onDelete fixture",
)
tests = replace_once(
    tests,
    "  group('Given a class with no database action explicitly set', () {\n",
    '\n  for (var actionKey in [\'onDelete\', \'onUpdate\']) {\n    group(\'Given $actionKey=SetNull\', () {\n      test(\'then a non-nullable explicit foreign key is rejected.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int\n  example: Example?, relation(field=exampleId, $actionKey=SetNull)\n\'\'\',\n        );\n\n        expect(collector.errors, hasLength(1));\n        expect(\n          collector.errors.single.message,\n          \'The foreign key field "exampleId" must be nullable in the database \'\n          \'when "$actionKey" is set to "SetNull".\',\n        );\n        expect(collector.errors.single.span?.text, actionKey);\n      });\n\n      test(\'then a nullable explicit foreign key is accepted.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int?\n  example: Example?, relation(field=exampleId, $actionKey=SetNull)\n\'\'\',\n        );\n\n        expect(collector.errors, isEmpty);\n      });\n\n      test(\'then a non-optional implicit foreign key is rejected.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  example: Example?, relation($actionKey=SetNull)\n\'\'\',\n        );\n\n        expect(collector.errors, hasLength(1));\n        expect(\n          collector.errors.single.message,\n          \'The foreign key field "exampleId" must be nullable in the database \'\n          \'when "$actionKey" is set to "SetNull".\',\n        );\n      });\n\n      test(\'then an optional implicit foreign key is accepted.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  example: Example?, relation($actionKey=SetNull, optional)\n\'\'\',\n        );\n\n        expect(collector.errors, isEmpty);\n      });\n    });\n\n    group(\'Given $actionKey=SetDefault\', () {\n      test(\'then a foreign key without a database default is rejected.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int\n  example: Example?, relation(field=exampleId, $actionKey=SetDefault)\n\'\'\',\n        );\n\n        expect(collector.errors, hasLength(1));\n        expect(\n          collector.errors.single.message,\n          \'The foreign key field "exampleId" must define a database default \'\n          \'using "default" or "defaultPersist" when "$actionKey" is set to \'\n          \'"SetDefault". "defaultModel" only initializes Dart objects and is \'\n          \'not applied by the database.\',\n        );\n        expect(collector.errors.single.span?.text, actionKey);\n      });\n\n      test(\'then defaultModel alone is rejected.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int, defaultModel=1\n  example: Example?, relation(field=exampleId, $actionKey=SetDefault)\n\'\'\',\n        );\n\n        expect(collector.errors, hasLength(1));\n        expect(\n          collector.errors.single.message,\n          contains(\'"defaultModel" only initializes Dart objects\'),\n        );\n      });\n\n      test(\'then defaultPersist is accepted.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int, defaultPersist=1\n  example: Example?, relation(field=exampleId, $actionKey=SetDefault)\n\'\'\',\n        );\n\n        expect(collector.errors, isEmpty);\n      });\n\n      test(\'then a shared default is accepted.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  exampleId: int, default=1\n  example: Example?, relation(field=exampleId, $actionKey=SetDefault)\n\'\'\',\n        );\n\n        expect(collector.errors, isEmpty);\n      });\n\n      test(\'then an implicit foreign key is rejected.\', () {\n        var collector = analyzeSingleModel(\n          \'\'\'\nclass: Example\ntable: example\nfields:\n  example: Example?, relation($actionKey=SetDefault)\n\'\'\',\n        );\n\n        expect(collector.errors, hasLength(1));\n        expect(\n          collector.errors.single.message,\n          contains(\'must define a database default\'),\n        );\n      });\n    });\n  }\n\n  group(\'Given a class with no database action explicitly set\', () {\n',
    "regression tests",
)
tests_path.write_text(tests)

changelog = changelog_path.read_text()
changelog = replace_once(
    changelog,
    '- fix: Adds missing export of `DeepCollectionEquality` for shared models. Backported to 3.4.11.\n',
    '- fix: Adds missing export of `DeepCollectionEquality` for shared models. Backported to 3.4.11.\n- fix: Validates `SetNull` and `SetDefault` relation actions against foreign key column constraints.\n',
    "changelog entry",
)
changelog_path.write_text(changelog)
