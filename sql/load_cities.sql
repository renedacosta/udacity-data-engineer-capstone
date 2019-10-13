WITH ct AS (
	SELECT DISTINCT ON (city) upper(city) as city
	FROM staging.city_temp as temp
	WHERE temp.country = 'United States'
)
INSERT INTO immigration.cities(city, state, country_id)
SELECT COALESCE(i94.city, ct.city) as city, state, c.id as country_id
FROM immigration.countries as c, staging.i94port as i94
FULL JOIN ct ON i94.city = ct.city
WHERE c.country = 'UNITED STATES'
