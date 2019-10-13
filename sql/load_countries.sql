INSERT INTO immigration.countries(country)
SELECT country
FROM staging.i94res;

INSERT INTO immigration.countries(country)
VALUES ('UNITED STATES')
