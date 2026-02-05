CREATE TABLE 
    lineitem_p19
AS 
    SELECT 
        *,
        l_extendedprice * (1 - l_discount) AS disc_price
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        (l_quantity >= 1
        AND l_quantity <= 1 + 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');
