{{
    config(
        materialized='table'
    )
}}

SELECT * 
FROM {{ source('globalmart', 'PRODUCT') }}