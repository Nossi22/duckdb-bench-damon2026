SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders_q04
WHERE
    EXISTS (
    SELECT
        *
    FROM
        lineitem_q04
    WHERE
        l_orderkey = o_orderkey)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
