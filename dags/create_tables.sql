CREATE TABLE IF NOT EXISTS public.staging_immigration (
     cicid FLOAT,
     i94yr FLOAT,
     i94mon FLOAT,
     i94cit FLOAT,
     i94res FLOAT,
     i94port VARCHAR,
     arrdate FLOAT,
     i94mode FLOAT,
     i94addr VARCHAR,
     depdate FLOAT,
     i94bir FLOAT,
     i94visa FLOAT,
     count FLOAT,
     dtadfile VARCHAR,
     visapost VARCHAR,
     occup VARCHAR,
     entdepa VARCHAR,
     entdepd VARCHAR,
     entdepu VARCHAR,
     matflag VARCHAR,
     biryear FLOAT,
     dtaddto VARCHAR,
     gender VARCHAR,
     insnum VARCHAR,
     airline VARCHAR,
     admnum FLOAT,
     fltno VARCHAR,
     visatype VARCHAR
);
     
CREATE TABLE IF NOT EXISTS public.states (
     state_code VARCHAR,
     state_name VARCHAR,
     median_age FLOAT,
     male_population FLOAT,
     female_population FLOAT,
     total_population INTEGER,
     number_of_veterans FLOAT,
     foreign_born FLOAT,
     household_size FLOAT,
     city_count INTEGER,
     CONSTRAINT states_pkey PRIMARY KEY (state_code)
);
     
CREATE TABLE IF NOT EXISTS public.airport_code (
     airport_code VARCHAR,
     airport_name VARCHAR,
     airport_state VARCHAR,
     CONSTRAINT airport_pkey PRIMARY KEY (airport_code)
);    
     
CREATE TABLE IF NOT EXISTS public.countries (
     country_code VARCHAR,
     country_name VARCHAR,
     CONSTRAINT countries_pkey PRIMARY KEY (country_code)
); 

CREATE TABLE IF NOT EXISTS public.staging_trans_mode (
     i94_mode INTEGER,
     transportation_mode VARCHAR,
     CONSTRAINT trans_mode_pkey PRIMARY KEY (i94_mode)
); 
     
CREATE TABLE IF NOT EXISTS public.staging_visa_code (
     i94_visa INTEGER,
     visa_type VARCHAR,
     CONSTRAINT visa_pkey PRIMARY KEY (i94_visa)
); 
     
CREATE TABLE IF NOT EXISTS public.arrivals (
     cicid INTEGER,
     state_code INTEGER,
     country_code INTEGER,
     airport_code VARCHAR,
     arrdate INTEGER,
     arrival_code VARCHAR,
     arrival_mode VARCHAR,
     state VARCHAR,
     airline VARCHAR,
     admnum BIGINT,
     fltno VARCHAR
);
     
CREATE TABLE IF NOT EXISTS public.admissions (
     admnum BIGINT,
     gender VARCHAR,
     age VARCHAR,
     biryear TIMESTAMP,
     visa_code INTEGER,
     visa_type VARCHAR,
     CONSTRAINT admissions_pkey PRIMARY KEY (admnum)
);

CREATE TABLE IF NOT EXISTS public.time (
    arrdate TIMESTAMP,
    month int4,
    year int4,
    week int4,
    day int4,
    weekday int4,
    CONSTRAINT time_pkey PRIMARY KEY (arrdate)
);
