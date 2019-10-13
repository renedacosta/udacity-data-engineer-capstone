INSERT INTO immigration.temperature(city_id, avg_temp)
SELECT DISTINCT ON (c.city) id as city_id, average_temperature as avg_temp
FROM staging.city_temp as ct
INNER JOIN immigration.cities as c ON c.city=upper(ct.city)
WHERE country = 'United States'
AND average_temperature is not null
