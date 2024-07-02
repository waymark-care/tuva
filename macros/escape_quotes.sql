{#
    The built-in dbt.string_literal doesn''t seem to escape quotes properly.
    This primarily affects postgres where ' must be entered as '' but on other platforms it is \'.
#}

{% macro escape_quotes(value) %}
{{ return(adapter.dispatch('escape_quotes', 'the_tuva_project')(value)) }}
{% endmacro %}



{% macro postgres__escape_quotes(value) %}
  {{ value | replace("'", "''") }}
{% endmacro %}



{% macro default__escape_quotes(value) %}
  {{ value | replace("'", "\\'") }}
{% endmacro %}
