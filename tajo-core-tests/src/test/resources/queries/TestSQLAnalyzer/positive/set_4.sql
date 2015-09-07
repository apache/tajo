SELECT *
FROM
(
  SELECT
          a.reg_date,
          a.user_id
  FROM
          (SELECT buy_date AS bdate
          FROM buy_history
          WHERE host='a0') as a JOIN (SELECT * FROM category_info WHERE category_id ='A1') as  b ON a.id=b.id
  UNION ALL
  SELECT
          a.reg_date,
          a.user_id
  FROM
          (SELECT buy_date AS bdate
          FROM buy_history
          WHERE host='b0') as a JOIN (SELECT * FROM category_info WHERE category_id ='B1') as b ON a.id=b.id
  UNION ALL
  SELECT
          a.reg_date,
          a.user_id
  FROM
          (SELECT buy_date AS bdate
          FROM buy_history
          WHERE host='c0') as a JOIN (SELECT * FROM category_info WHERE category_id ='C1') as  b ON a.id=b.id
  UNION ALL
  SELECT
          a.reg_date,
          a.user_id
  FROM
          (SELECT buy_date AS bdate
          FROM buy_history
          WHERE host='d0') as  a JOIN (SELECT * FROM category_info WHERE category_id ='D1') as  b ON a.id=b.id

)  as T