CREATE TABLE 
    orders_q21
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderstatus = 'F';

CREATE TABLE 
    nation_q21
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'SAUDI ARABIA';
