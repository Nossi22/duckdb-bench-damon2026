CREATE TABLE 
    lineitem_p15
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
        AND l_shipdate < CAST('1996-04-01' AS date);
