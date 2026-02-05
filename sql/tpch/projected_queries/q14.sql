SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            disc_price
        ELSE
            0
        END) / sum(disc_price) AS promo_revenue
FROM
    lineitem_p14,
    part
WHERE
    l_partkey = p_partkey;
