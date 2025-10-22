env = "{{ data.env }}"

{% for key, value in data.items() %}
{% if key != "env" %}
[{{ key }}]
    [{{ key }}.{{ data.env }}]
    host="{{ value.host }}"
    port="{{ value.port }}"
    password="{{ value.password }}"
    maxIdle=100
    maxOpen={{ value.maxOpen|default(3000) }}
    connect_timeout=5
    read_timeout=5
    write_timeout=5
    idle_timeout=300
{% endif %}
{% endfor %}
