WITH cities AS (
 	SELECT ic.id as city_id, st.id as i94port
	FROM immigration.cities as ic
	INNER JOIN staging.i94port as st ON ic.city = st.city AND ic.state = st.state
), 
countries AS (
 	SELECT ic.id as country_id, st.id as i94res
	FROM immigration.countries as ic
	INNER JOIN staging.i94res as st ON ic.country = st.country
)
INSERT INTO immigration.immigrants(
	i94yr,i94mon,i94cit,i94res,i94port,
	gender,biryear,arrdate,i94mode,depdate,i94visa
)
SELECT 
	CAST(i94yr as int) as i94yr,
	CAST(i94mon as int) as i94mon,
	co2.country_id as i94cit,
	co1.country_id as i94res,
	ci.city_id as i94port,
	gender,
	CAST(biryear as int) as biryear,
	to_timestamp(1451653201 + arrdate-20454) as arrdate,
	CAST(i94mode as int) as i94mode,
	to_timestamp(1451653201 + depdate-20454) as depdate,
	CAST(i94visa as int) as i94visa
FROM staging.immigration as immi
LEFT JOIN countries as co1 ON immi.i94res = co1.i94res
LEFT JOIN countries as co2 ON immi.i94res = co2.i94res
RIGHT JOIN cities as ci ON immi.i94port = ci.i94port
WHERE i94cit is not null
AND co1.i94res is not null
AND ci.i94port is not null
AND gender in ('M', 'F')
AND arrdate is not null
AND depdate is not null
