--
-- Create inital schema 
--
DROP SCHEMA IF EXISTS immigration CASCADE;
CREATE SCHEMA IF NOT EXISTS immigration;
COMMENT ON SCHEMA immigration IS 'Immigration Schema';

DROP SCHEMA IF EXISTS summary CASCADE;
CREATE SCHEMA IF NOT EXISTS summary;
COMMENT ON SCHEMA summary IS 'Summary Schema';

SET search_path = public, immigration;

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
-- countries dimension table
CREATE TABLE IF NOT EXISTS immigration.countries (
    id                          serial primary key,
    country                     character varying not null UNIQUE
);

-- cities dimension table
CREATE TABLE IF NOT EXISTS immigration.cities (
    id                          serial primary key,
    city                        character varying not null,
    state                       character varying,
    country_id                  int not null REFERENCES immigration.countries(id),
    avg_temp                    numeric not null,
    UNIQUE(city, state)
);

-- visa type
CREATE TABLE IF NOT EXISTS immigration.visas (
    id                          int primary key,
    visa                        character varying not null UNIQUE
);

INSERT INTO immigration.visas
VALUES
    (1 , 'Business'),
    (2 , 'Pleasure'),
    (3 , 'Student');

-- travel modes type
CREATE TABLE IF NOT EXISTS immigration.modes (
    id                          int primary key,
    mode                        character varying not null UNIQUE
);

INSERT INTO immigration.modes
VALUES
    (1 , 'Air'),
    (2 , 'Sea'),
    (3 , 'Land'),
    (9 , 'Not reported');

-- immigration fact table
CREATE TABLE IF NOT EXISTS immigration.immigrants (
    id                          serial primary key,
    i94yr                       int not null,
    i94mon                      int not null,
    i94cit                      int not null REFERENCES immigration.countries(id),
    i94res                      int not null REFERENCES immigration.countries(id),
    i94port                     int not null REFERENCES immigration.cities(id),
    gender                      char(1),
    biryear                     int not null,
    arrdate                     timestamp with time zone not null,
    i94mode                     int not null REFERENCES immigration.modes(id),
    depdate                     timestamp with time zone not null,
    i94visa                     int not null REFERENCES immigration.visas(id),
    CHECK ((gender = 'F') or (gender = 'M') or (gender is null))
);

CREATE TABLE IF NOT EXISTS summary.summary (
    id                          serial primary key,
    i94yr                       int not null,
    i94mon                      int not null,
    i94res                      varchar not null,
    i94port                     varchar not null,
    i94mode                     varchar not null,
    i94visa                     varchar not null,
    avg_temp                    double precision not null,
    count                       int not null
);
