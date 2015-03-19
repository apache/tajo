SELECT
  user.name,
  sum(retweet_count) as total_retweet
FROM
  tweets
GROUP BY
  user.name;