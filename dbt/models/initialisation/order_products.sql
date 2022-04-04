(
    select *
    from {{ source('instacart_raw_data', 'order_products__train') }}
)
UNION 
(   
    select *
    from {{ source('instacart_raw_data', 'order_products__train') }}
)