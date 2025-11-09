with cleaned_sales as (
  select
  *
from
  {{ source('source', 'fact_sales') }}
  where sales_id is not null
),


deduped_sales as (

  select
  *,
  row_number() over (partition by sales_id order by date_sk desc) as rn
  from
    cleaned_sales
  )

select
*
from deduped_sales
where rn = 1