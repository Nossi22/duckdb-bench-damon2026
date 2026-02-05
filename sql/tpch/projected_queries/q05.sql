SELECT
    n_name,
    sum(disc_price) AS revenue
FROM
    customer,
    orders_f05,
    lineitem_p05,
    supplier,
    nation,
    region_f05
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
GROUP BY
    n_name
ORDER BY
    revenue DESC;
