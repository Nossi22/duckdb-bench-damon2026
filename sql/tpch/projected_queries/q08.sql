SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        o_year,
        volume,
        n2.n_name AS nation
    FROM
        part_f08,
        supplier,
        lineitem_p08,
        orders_p08,
        customer,
        nation n1,
        nation n2,
        region_f08
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND s_nationkey = n2.n_nationkey) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
