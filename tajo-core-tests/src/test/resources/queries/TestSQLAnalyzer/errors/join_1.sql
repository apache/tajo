SELECT
  d_date_sk ss_sold_date_sk
FROM s_purchase
  LEFTT OUTER JOIN customer ON (purc_customer_id = c_customer_id)
  LEFT OUTER JOIN store ON (purc_store_id = s_store_id)
  LEFT OUTER JOIN date_dim ON (cast(purc_purchase_date as date) = d_date)
  LEFT OUTER JOIN time_dim ON (PURC_PURCHASE_TIME = t_time)
  JOIN s_purchase_lineitem ON (purc_purchase_id = plin_purchase_id)
  LEFT OUTER JOIN promotion ON plin_promotion_id = p_promo_id
  LEFT OUTER JOIN item ON plin_item_id = i_item_id
WHERE purc_purchase_id = plin_purchase_id
  AND i_rec_end_date is NULL
  AND s_rec_end_date is NULL;