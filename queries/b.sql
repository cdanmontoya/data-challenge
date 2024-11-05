SELECT country, name, count FROM (
  SELECT
    country,
    name,
    count(*) as count,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY COUNT(name) DESC) AS row_num
  FROM (
    SELECT first_name as name, country FROM `meli-challenge-440721.names.names_dataset`
    UNION ALL (SELECT first_last_name, country FROM `meli-challenge-440721.names.names_dataset`)
    UNION ALL (SELECT second_last_name, country FROM `meli-challenge-440721.names.names_dataset` WHERE second_last_name IS NOT NULL)
  )
  WHERE country is not null
  GROUP BY country, name
)
WHERE row_num = 1