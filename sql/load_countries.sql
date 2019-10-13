INSERT INTO immigration.countries(country)
    SELECT country
    FROM staging.i94res
ON CONFLICT ON CONSTRAINT countries_country_key DO NOTHING;

INSERT INTO immigration.countries(country)
VALUES ('UNITED STATES')
ON CONFLICT ON CONSTRAINT countries_country_key DO NOTHING;
