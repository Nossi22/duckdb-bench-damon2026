CREATE TABLE 
    lineitem_p10
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS volume,
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_returnflag = 'R';
