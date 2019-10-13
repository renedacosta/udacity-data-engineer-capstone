INSERT INTO summary.summary(i94yr, i94mon, i94res, i94port, i94mode, i94visa, avg_temp, count)
SELECT 
	i94yr, 
	i94mon, 
	co.country as i94res, 
	ci.city as i94port, 
	mode as i94mode, 
	visa as i94visa, 
	avg_temp, 
	count(*) as count
FROM immigration.immigrants as immi
INNER JOIN immigration.countries as co ON immi.i94res=co.id
INNER JOIN immigration.cities as ci ON immi.i94port=ci.id
INNER JOIN immigration.visas as vi ON immi.i94visa=vi.id
INNER JOIN immigration.modes as mo ON immi.i94mode=vi.id
GROUP BY i94yr, i94mon, co.country, ci.city, mode, visa, avg_temp
