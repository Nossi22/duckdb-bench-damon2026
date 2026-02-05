CREATE TABLE 
    lineitem_p03
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate > CAST('1995-03-15' AS date);
