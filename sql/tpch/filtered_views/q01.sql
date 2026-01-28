CREATE TABLE 
    lineitem_q01 
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate <= CAST('1998-09-02' AS date);