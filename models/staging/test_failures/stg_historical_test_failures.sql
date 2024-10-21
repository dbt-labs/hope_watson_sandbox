with base as (
    select
        sk_id,
        test_name,
        test_node,
        test_failures_json,
        _timestamp,
        model_names,  -- keep the original array
        
        -- Use Jinja to dynamically create the model dependency columns
        {% for i in range(0, 5) %}
            case
                when array_size(model_names) > {{ i }} then
                    regexp_replace(
                        split_part(model_names[{{ i }}], '.', 3), '_v[0-9]+$', ''
                    )
                else null
            end as model_name_test_dependency_{{ i + 1 }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ source('test_failures', 'test_failure_history') }}
)

select
    sk_id,
    test_name,
    test_node,
    test_failures_json,
    _timestamp,
    model_names,  -- output the original array as-is
    -- Use Jinja to dynamically select the model dependency columns
    {% for i in range(0, 5) %}
        model_name_test_dependency_{{ i + 1 }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from base
