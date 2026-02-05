SELECT
    l_orderkey,
    sum(disc_price) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer_f03,
    orders_f03,
    lineitem_p03
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
