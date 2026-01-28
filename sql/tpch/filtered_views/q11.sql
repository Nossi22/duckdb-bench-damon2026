CREATE TABLE 
    nation_q11
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'GERMANY';
