CREATE TABLE 
    orders_p08
AS 
    SELECT 
        *,
        extract(year FROM o_orderdate) AS o_year
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date);

CREATE TABLE 
    lineitem_p08
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS volume
    FROM 
        read_parquet('data/tpch/lineitem.parquet');
