{#
This model represents the time standards for packing operations
The assumption made here is thateach of these actions occurs once, for each order that is packed
#}

with
    raw_ts as (
        {{
            dbt_utils.unpivot(
                relation=ref("packing_time_standards"),
                cast_to="varchar",
                field_name="operation",
                value_name="time_standard",
            )
        }}
    ),
    t_dept as (
        select replace(replace(raw_ts.operation, '_standard', ''), 'basic', '') as operation, time_standard
        from raw_ts
        where operation like 'basic%'
    )
select operation, time_standard
from t_dept


