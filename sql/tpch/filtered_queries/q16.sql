SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part_q16
WHERE
    p_partkey = ps_partkey
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier_q16)
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;
