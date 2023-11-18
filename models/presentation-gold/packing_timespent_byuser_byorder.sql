/*
Assumptions:
    - The time it took to pack was the time between one transaction and the next one, for a given user
    - As such, we cannot estimate the pack time for the last order
    - Because an entire order has the same timestamp, we can only calculate the time to pack an entire order, not a given article
    - This model does not take into account the fact that the user needs to take breaks, break expectations should be added

Confirmation of assumptions:

- Users who are packing orders only pack orders, they are not moving in between tasks: 

select distinct
    user_id,
    action_code
from {{ref('dim_transactions')}}
where user_id in (select user_id from {{ref('dim_transactions')}} where action_code = 'PKOCLOSE')
order by 1

This allows me to create a CTE isolating only packing transactions, and trust that subsequent tasks in this subsquery happened sequentially

- A user will always start packing an entire order at the same time:

select 
    order_number,
    user_id,
    count(distinct transaction_timestamp)
from {{ ref("dim_transactions") }}
where action_code = 'PKOCLOSE'
group by 1,2
order by 3 desc 

This allows me to select distinct orders and users, in order to calculate the time spent on each order packed

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
    pack_article_standards as (
        select article, department, time_standard
        from {{ ref("lookup_packing_article_standards") }} 
    ),
    pack_process_standards as (
        select sum(time_standard) as order_process_time_standard
        from {{ref('lookup_packing_process_standards')}}
    ),
    time_spent_articles_expected as (
        select order_number, sum(time_standard * quantity) as time_expected
        from pack_orders po
        left join pack_article_standards ps on po.article = ps.article
        group by order_number
    ),
    time_spent_expected as (
        select order_number, time_expected + order_process_time_standard as time_expected
        from time_spent_articles_expected
        left join pack_process_standards on 1=1 -- join this value for all order
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


    
