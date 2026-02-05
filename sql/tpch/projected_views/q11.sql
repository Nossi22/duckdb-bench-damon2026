CREATE TABLE 
    partsupp_p11
AS 
    SELECT 
        *,
        ps_supplycost * ps_availqty AS ps_value
    FROM 
        read_parquet('data/tpch/partsupp.parquet');
