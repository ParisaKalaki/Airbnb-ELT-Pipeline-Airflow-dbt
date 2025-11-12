{% macro generate_schema_name(custom_schema_name, node) -%}

    {# This variable stores the default schema from your profiles.yml (e.g., 'bronze') #}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# If no custom schema is set in dbt_project.yml, use the default profile schema #}
        {{ default_schema }}

    {%- else -%}

        {# If a custom schema is set (e.g., 'silver'), use ONLY the custom schema name #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
