SELECT
-- FROM raw orders
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
{{ markup('O.ORDERSELLINGPRICE','O.ORDERCOSTPRICE') }} as markup
FROM {{ ref('raw_orders') }} AS O
LEFT JOIN {{ ref('raw_customer') }} AS C
ON O.CUSTOMERID = C.CUSTOMERID
LEFT JOIN {{ ref('raw_product') }} AS P
ON O.PRODUCTID = P.PRODUCTID