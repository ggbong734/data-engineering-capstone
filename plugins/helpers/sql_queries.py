class SqlQueries:

    arrivals_table_insert = ("""
    SELECT CAST(im.cicid AS INT),
           CAST(im.i94cit AS INT) AS state_code,
           CAST(im.i94res AS INT) AS country_code,
           im.i94port AS airport_code,
           CAST(arrdate AS INT),
           CAST(im.i94mode AS INT) AS arrival_code,
           tm.transportation_mode AS arrival_mode,
           im.i94addr AS state,
           im.airline,
           CAST(im.admnum AS BIGINT),
           im.fltno
        FROM staging_immigration AS im
        LEFT JOIN staging_trans_mode AS tm
        ON CAST(im.i94mode AS INT) = tm.i94_mode
    """)

    admissions_table_insert = ("""
    SELECT CAST(im.admnum AS BIGINT),
           im.gender,
           CAST(im.i94bir AS INT),
           to_timestamp(CAST(im.biryear AS INT), 'YYYY') as birth_year,
           CAST(im.i94visa AS INT),
           vs.visa_type
        FROM staging_immigration AS im
        LEFT JOIN staging_visa_code AS vs
        ON CAST(im.i94visa AS INT) = vs.visa_type
    """)

    time_table_insert = ("""
    SELECT dates.arrtime AS arrdate,
           extract(year from dates.arrtime),
           extract(month from dates.arrtime),
           extract(week from dates.arrtime), 
           extract(day from dates.arrtime),
           extract(dayofweek from dates.arrtime) AS weekday
        FROM (SELECT DATEADD(day, CAST(arrdate AS INT), '1900-01-01') AS arrtime
            FROM staging_immigration) dates
    """)
    
    