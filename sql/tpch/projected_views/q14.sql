CREATE TABLE 
    lineitem_p14
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= date '1995-09-01'
        AND l_shipdate < CAST('1995-10-01' AS date);
