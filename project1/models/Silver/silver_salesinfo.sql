with sales as (

    select
      sales_id,
      product_sk,
      customer_sk,
      {{multiply ('unit_price', 'quantity')}} as calculated_gross_amount,
      payment_method,
        gross_amount
    from
      {{ ref('bronze_sales') }}

),

products as (

    select
      product_sk,
      category
    from 
      {{ ref('bronze_product') }}

),

customer as(

    select
      customer_sk,
      gender
    from 
      {{ ref('bronze_customer') }}
),

joined_query as (

SELECT 
    sales.sales_id,
    sales.payment_method,
    sales.gross_amount,
    products.category,
    customer.gender

FROM 
    sales
join
    products on sales.product_sk = products.product_sk
join
    customer on sales.customer_sk = customer.customer_sk
)


select 
    gender,
    category,
    sum(gross_amount) as total_sales

from
    joined_query
group by
    gender,
    category
order by
    total_sales desc