select
    transaction_id,
    warehouse_id,
    transaction_timestamp::datetime,
    "date"::date as transaction_date,
    "hour"::int as transaction_hour,
    order_type,
    order_type_category,
    order_channel,
    user_id,
    action_code,
    order_number,
    order_line_item::int,
    article,
    device_code,
    ship_line_id,
    gift_flag,
    from_area_code,
    to_area_code,
    from_storage_location,
    to_storage_location,
    order_category,
    inventory_detail_number,
    quantity::int

from {{ ref("transactions") }}
-- there is some bad data here - filtering out rows with no quantities
where quantity ~ '[*0-9]'
