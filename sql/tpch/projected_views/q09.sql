CREATE TABLE 
    orders_p09
AS 
    SELECT 
        *,
        extract(year FROM o_orderdate) AS o_year
    FROM 
        read_parquet('data/tpch/orders.parquet');

CREATE TABLE 
    lineitem_p09
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS volume
    FROM 
        read_parquet('data/tpch/lineitem.parquet');
