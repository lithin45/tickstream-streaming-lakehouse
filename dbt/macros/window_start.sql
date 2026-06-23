{# Epoch-aligned tumbling-window start, identical to processing/metrics.window_bounds:
   floor(epoch(ts) / seconds) * seconds, as a UTC timestamp. #}
{% macro window_start(ts_col, seconds) %}
  to_timestamp(floor(epoch({{ ts_col }}) / {{ seconds }}) * {{ seconds }})
{% endmacro %}
