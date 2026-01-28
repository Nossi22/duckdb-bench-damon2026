CREATE VIEW 
    lineitem 
AS 
    SELECT * FROM read_parquet('data/tpch/lineitem.parquet');