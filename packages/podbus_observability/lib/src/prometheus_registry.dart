import 'dart:collection';

import 'package:podbus_core/podbus_core.dart';

enum PrometheusMetricKind { counter, gauge, summary }

final class PrometheusRegistry {
  PrometheusRegistry({
    this.maxSeries = 2000,
    Set<String>? allowedLabelKeys,
    PrometheusMetricKind Function(MessagingMetricEvent event)? classify,
  }) : allowedLabelKeys = Set.unmodifiable(
         allowedLabelKeys ??
             const {
               'transport',
               'topic',
               'subject',
               'exchange',
               'routingKey',
               'status',
               'unit',
             },
       ),
       _classify = classify ?? _defaultClassifier {
    if (maxSeries < 1) {
      throw const MessagingConfigurationException(
        'Prometheus maxSeries must be greater than zero.',
      );
    }
  }

  final int maxSeries;
  final Set<String> allowedLabelKeys;
  final PrometheusMetricKind Function(MessagingMetricEvent event) _classify;
  final Map<_SeriesKey, _MetricValue> _series = LinkedHashMap();
  var _droppedSeries = 0;

  int get seriesCount => _series.length;
  int get droppedSeries => _droppedSeries;

  MessagingMetricHook get hook => record;

  void record(MessagingMetricEvent event) {
    final kind = _classify(event);
    final name = _metricName(event.name, kind);
    final labels = <String, String>{
      for (final entry in event.attributes.entries)
        if (allowedLabelKeys.contains(entry.key) && entry.value != null)
          _sanitizeLabelName(entry.key): entry.value.toString(),
    };
    final key = _SeriesKey(name, kind, labels);
    final existing = _series[key];
    if (existing == null && _series.length >= maxSeries) {
      _droppedSeries += 1;
      return;
    }
    final value = existing ?? _MetricValue();
    switch (kind) {
      case PrometheusMetricKind.counter:
        value.value += event.value.toDouble();
      case PrometheusMetricKind.gauge:
        value.value = event.value.toDouble();
      case PrometheusMetricKind.summary:
        value.value += event.value.toDouble();
        value.count += 1;
    }
    _series[key] = value;
  }

  void clear() {
    _series.clear();
    _droppedSeries = 0;
  }

  String render() {
    final buffer = StringBuffer();
    final sorted = _series.entries.toList()
      ..sort((left, right) => left.key.compareTo(right.key));
    final emittedTypes = <String>{};
    for (final entry in sorted) {
      final key = entry.key;
      final value = entry.value;
      if (emittedTypes.add(key.name)) {
        buffer.writeln('# TYPE ${key.name} ${_typeName(key.kind)}');
      }
      final labels = _renderLabels(key.labels);
      if (key.kind == PrometheusMetricKind.summary) {
        buffer.writeln('${key.name}_sum$labels ${_number(value.value)}');
        buffer.writeln('${key.name}_count$labels ${value.count}');
      } else {
        buffer.writeln('${key.name}$labels ${_number(value.value)}');
      }
    }
    buffer.writeln('# TYPE podbus_metrics_dropped_series_total counter');
    buffer.writeln('podbus_metrics_dropped_series_total $_droppedSeries');
    return buffer.toString();
  }

  static PrometheusMetricKind _defaultClassifier(MessagingMetricEvent event) {
    final name = event.name.toLowerCase();
    if (name.contains('duration') ||
        name.endsWith('.latency') ||
        event.attributes['unit'] == 'microseconds') {
      return PrometheusMetricKind.summary;
    }
    if (name.endsWith('.active') ||
        name.endsWith('.pending') ||
        name.endsWith('.lag') ||
        name.endsWith('.depth') ||
        name.endsWith('.size')) {
      return PrometheusMetricKind.gauge;
    }
    return PrometheusMetricKind.counter;
  }

  static String _metricName(String value, PrometheusMetricKind kind) {
    var name = value
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9_:]'), '_')
        .replaceAll(RegExp('_+'), '_');
    if (!RegExp(r'^[a-z_:]').hasMatch(name)) {
      name = 'podbus_$name';
    }
    if (kind == PrometheusMetricKind.counter && !name.endsWith('_total')) {
      name = '${name}_total';
    }
    return name;
  }

  static String _sanitizeLabelName(String value) {
    var label = value
        .replaceAll(RegExp(r'[^a-zA-Z0-9_]'), '_')
        .replaceAll(RegExp('_+'), '_');
    if (!RegExp(r'^[a-zA-Z_]').hasMatch(label)) {
      label = 'label_$label';
    }
    return label;
  }

  static String _renderLabels(Map<String, String> labels) {
    if (labels.isEmpty) {
      return '';
    }
    final sorted = labels.entries.toList()
      ..sort((left, right) => left.key.compareTo(right.key));
    return '{${sorted.map((entry) => '${entry.key}="${_escape(entry.value)}"').join(',')}}';
  }

  static String _escape(String value) {
    return value
        .replaceAll(r'\', r'\\')
        .replaceAll('\n', r'\n')
        .replaceAll('"', r'\"');
  }

  static String _number(double value) {
    if (value.isNaN) {
      return 'NaN';
    }
    if (value == double.infinity) {
      return '+Inf';
    }
    if (value == double.negativeInfinity) {
      return '-Inf';
    }
    return value.toString();
  }

  static String _typeName(PrometheusMetricKind kind) => switch (kind) {
    PrometheusMetricKind.counter => 'counter',
    PrometheusMetricKind.gauge => 'gauge',
    PrometheusMetricKind.summary => 'summary',
  };
}

final class _SeriesKey implements Comparable<_SeriesKey> {
  _SeriesKey(this.name, this.kind, Map<String, String> labels)
    : labels = Map.unmodifiable(labels),
      _identity = _identityFor(name, kind, labels);

  final String name;
  final PrometheusMetricKind kind;
  final Map<String, String> labels;
  final String _identity;

  @override
  int compareTo(_SeriesKey other) => _identity.compareTo(other._identity);

  @override
  bool operator ==(Object other) =>
      other is _SeriesKey && other._identity == _identity;

  @override
  int get hashCode => _identity.hashCode;

  static String _identityFor(
    String name,
    PrometheusMetricKind kind,
    Map<String, String> labels,
  ) {
    final sorted = labels.entries.toList()
      ..sort((left, right) => left.key.compareTo(right.key));
    return '$name|${kind.name}|${sorted.map((entry) => '${entry.key}=${entry.value}').join('|')}';
  }
}

final class _MetricValue {
  double value = 0;
  int count = 0;
}
