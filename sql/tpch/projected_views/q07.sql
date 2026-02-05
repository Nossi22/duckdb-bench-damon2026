CREATE TABLE 
    lineitem_p07
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS volume,
        extract(year FROM l_shipdate) AS l_year
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date);
