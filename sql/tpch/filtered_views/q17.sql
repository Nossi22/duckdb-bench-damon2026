CREATE TABLE 
    part_q17
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_brand = 'Brand#23'
        AND p_container = 'MED BOX';
