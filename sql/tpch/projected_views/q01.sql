CREATE TABLE 
    lineitem_p01 
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price,
        l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate <= CAST('1998-09-02' AS date);