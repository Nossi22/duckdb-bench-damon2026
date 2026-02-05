SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        o_year,
        volume - ps_supplycost * l_quantity AS amount
    FROM
        part_f09,
        supplier,
        lineitem_p09,
        partsupp,
        orders_p09,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
