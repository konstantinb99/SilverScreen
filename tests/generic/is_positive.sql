-- tests/generic/is_positive.sql

-- Dies ist ein generischer Test, der überprüft, ob alle Werte in einer Spalte nicht negativ sind.
-- Er wird fehlschlagen, wenn ein Wert kleiner als 0 gefunden wird.

{% test is_positive(model, column_name) %}

SELECT
    *
FROM
    {{ model }}
WHERE
    {{ column_name }} < 0

{% endtest %}
