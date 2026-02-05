SELECT
    ps_partkey,
    sum(ps_value) AS value
FROM
    partsupp_p11,
    supplier,
    nation_f11
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
GROUP BY
    ps_partkey
HAVING
    sum(ps_value) > (
        SELECT
            sum(ps_value) * 0.0001000000
        FROM
            partsupp_p11,
            supplier,
            nation_f11
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey)
ORDER BY
    value DESC;
