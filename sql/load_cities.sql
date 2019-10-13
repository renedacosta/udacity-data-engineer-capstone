WITH ct AS (
	SELECT DISTINCT ON (city) upper(city) as city, average_temperature as avg_temp
	FROM staging.city_temp as temp
	WHERE temp.country = 'United States'
)
INSERT INTO immigration.cities(city, state, country_id, avg_temp)
	SELECT DISTINCT COALESCE(i94.city, ct.city) as city, state, c.id as country_id, avg_temp
	FROM immigration.countries as c, staging.i94port as i94
	FULL JOIN ct ON i94.city = ct.city
	WHERE c.country = 'UNITED STATES'
	AND avg_temp is not null
ON CONFLICT ON CONSTRAINT cities_city_state_key DO NOTHING;

