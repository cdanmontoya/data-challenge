SELECT country, COUNT(*) FROM `meli-challenge-440721.names.names_dataset`
WHERE age > 0 AND age < 30
GROUP BY country
ORDER BY COUNT(*) DESC
LIMIT 1