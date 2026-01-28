CREATE TABLE 
    customer_q03 
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/customer.parquet')
    WHERE
        c_mktsegment = 'BUILDING';

CREATE TABLE 
    orders_q03
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate < CAST('1995-03-15' AS date);

CREATE TABLE 
    lineitem_q03
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate > CAST('1995-03-15' AS date);
