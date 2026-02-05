CREATE TABLE 
    lineitem_p05
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price
    FROM 
        read_parquet('data/tpch/lineitem.parquet');
