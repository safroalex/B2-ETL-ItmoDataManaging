{% test valid_temperature(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < -100 or {{ column_name }} > 60
{% endtest %}
