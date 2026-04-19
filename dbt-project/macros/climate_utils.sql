{% macro case_for_number_of_days_in_month(col_name) %}
    CASE WHEN {{ col_name }} > 31 THEN NULL
         ELSE CAST({{ col_name }} AS INTEGER)
    END
{% endmacro %}



{% macro apply_sign_and_adjust_tenths(col_name, sign_col_name) %}
    CASE
        WHEN {{col_name}}  IS NULL THEN NULL
        WHEN {{sign_col_name}}   = 1   THEN -({{col_name}}  / 10.0)
        ELSE {{col_name}}  / 10.0
    END
{% endmacro %}