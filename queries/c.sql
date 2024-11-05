SELECT country, AVG(age) AS average_age
FROM `meli-challenge-440721.names.names_dataset`
WHERE ranking = 'UPPER' AND age > 0
GROUP BY country