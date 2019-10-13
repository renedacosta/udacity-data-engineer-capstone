--
-- Create inital schema 
--
DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Staging Schema';

SET search_path = public, staging;

-- General statements
SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'ISO-8859-1';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--
-- Create Tables
--
-- temperature
CREATE TABLE staging.city_temp (
    dt                              timestamp,
    average_temperature             numeric,
    average_temperatureUncertainty  numeric,
    city                            varchar,
    country                         varchar,
    latitude                        varchar,
    longitude                       varchar
);

-- immigation
CREATE TABLE staging.immigration (
    i94yr       numeric,
    i94mon      numeric,
    i94cit      numeric,
    i94res      numeric,
    i94port     varchar,
    arrdate     numeric,
    i94mode     numeric,
    depdate     numeric,
    i94bir      numeric,
    i94visa     numeric,
    gender      varchar,
    biryear     numeric
);

CREATE TABLE staging.i94res(
    id          int,
    country     varchar
);

CREATE TABLE staging.i94port(
    id          varchar,
    city        varchar,
    state       varchar
)
