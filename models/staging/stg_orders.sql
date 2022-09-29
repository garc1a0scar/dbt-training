SELECT
-- FROM raw orders
{{ dbt_utils.surrogate_key(['o.orderid','c.customerid','p.productid']) }} as sk_orders,
O.ORDERID,
O.ORDERDATE,
O.SHIPDATE,
O.SHIPMODE,
O.ORDERSELLINGPRICE - O.ORDERCOSTPRICE as ORDERPROFIT,
O.ORDERCOSTPRICE,
O.ORDERSELLINGPRICE,
--FROM raw_customer
C.CUSTOMERID,
C.CUSTOMERNAME,
C.SEGMENT,
C.COUNTRY,
--FROM raw_product
P.CATEGORY,
P.PRODUCTNAME,
P.SUBCATEGORY,
P.PRODUCTID,
{{ markup('O.ORDERSELLINGPRICE','O.ORDERCOSTPRICE') }} as markup,
D.delivery_team
FROM {{ ref('raw_orders') }} AS O
LEFT JOIN {{ ref('raw_customer') }} AS C
ON O.CUSTOMERID = C.CUSTOMERID
LEFT JOIN {{ ref('raw_product') }} AS P
ON O.PRODUCTID = P.PRODUCTID
LEFT JOIN {{ ref('delivery_team') }} D
ON O.shipmode = d.shipmode