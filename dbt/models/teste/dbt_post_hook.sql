{{
    config(
        schema = "refined",
        post_hook = after_commit("DELETE FROM {{this}} WHERE order_id = 2539329")
    )
}}

select * from {{ ref ('dbt_clean_orders') }}