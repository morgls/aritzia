/*
Assumptions:
    - The time it took to pack was the time between one transaction and the next one, for a given user
    - As such, we cannot estimate the pack time for the last order
    - Because an entire order has the same timestamp, we can only calculate the time to pack an entire order, not a given article
    - This model does not take into account the fact that the user needs to take breaks, break expectations should be added
*/

with
    pack_orders as (
        select
            user_id,
            transaction_id,
            order_number,
            transaction_date as pack_date,
            transaction_timestamp as pack_timestamp,
            article,
            quantity
        from {{ ref("dim_transactions") }}
        where action_code = 'PKOCLOSE'
    ),
    distinct_pack_orders as (
        select distinct
            user_id,
            order_number,
            pack_date,
            pack_timestamp,
            dense_rank() over (
                partition by user_id, pack_date order by pack_timestamp asc
            ) as shift_order_packed
        from pack_orders
    ),
    time_spent_actual as (
        select
            user_id,
            order_number,
            shift_order_packed,
            pack_date,
            pack_timestamp,
            lead(pack_timestamp, 1) over (
                partition by user_id, pack_date order by pack_timestamp asc
            ) as next_timestamp,
            datediff(second, pack_timestamp, next_timestamp) as time_spent_packing
        from distinct_pack_orders
    ),
    pack_standards as (
        select a.material as article, a.department, time_standard
        from {{ ref("dim_articles") }} a
        left join {{ ref("lookup_packing_standards") }} s on a.department = s.department
    ),
    time_spent_expected as (
        select order_number, sum(time_standard * quantity) as time_expected
        from pack_orders po
        left join pack_standards ps on po.article = ps.article
        group by order_number
    )
select
    actual.pack_date,
    actual.user_id,
    actual.order_number,
    actual.time_spent_packing,
    expected.time_expected
from time_spent_actual actual
left join time_spent_expected expected on actual.order_number = expected.order_number
where time_spent_packing is not null  -- we can't estimate the packing time for the last order of the day
order by
    actual.pack_date,
    actual.user_id


    
