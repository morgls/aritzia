{#
This model represents the time standards for packing specific articles
The assumption made here is thateach of these actions occurs once, per article, when packing
#}

with
    article_dept as (
        select distinct
            department,
            material as article,
            -- convert to lowercase and remove any non-word characters
            regexp_replace(lower(department), '[^a-z]', '') as operation
        from {{ ref("dim_articles") }}
        order by department
    ),
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
        select replace(raw_ts.operation, '_standard', '') as operation, time_standard
        from raw_ts
    )
select department, article, time_standard
from article_dept
left join
    t_dept
    on (
        article_dept.operation = t_dept.operation
        -- this is a manual mapping and a risk: the assumption being made here is that this lookup table rarely changes
        -- in a production environment I would add a test that flags any departments that do not have a packing time standard mapped
        or (t_dept.operation = 'smallgood' and article_dept.operation = 'facemasks')
    )
