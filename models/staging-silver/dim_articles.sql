select
    material,
    department
from {{ref('article_master_table')}}
