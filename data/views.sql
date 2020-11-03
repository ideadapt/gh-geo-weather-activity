-- ALL GH PROFILE COUNTRIES THAT ARE NOAA COUNTRIES

REFRESH MATERIALIZED VIEW countries_in_profiles;
CREATE MATERIALIZED VIEW countries_in_profiles
AS
SELECT
    DISTINCT ON (addr.shortname) addr.shortname,
                                 addr.profile_location,
                                 addr.comp,
                                 addr.shortname,
                                 nct.country_id AS n_shortname,
                                 nct.id AS n_id

FROM ( SELECT l.location AS profile_location,
              l.data #> '{0,address_components}'::text[] AS comp,
              btrim((l.data #> '{0,address_components,0,short_name}'::text[])::text, '"'::text) AS shortname
       FROM locations l) addr
         JOIN ( SELECT nc.id,
                       substr(nc.id, 6, 2) AS country_id,
                       nc.name
                FROM countries_typed_view nc) nct ON nct.country_id = addr.shortname
WHERE addr.comp @> '[{"types": ["country"]}]'::jsonb AND jsonb_array_length(addr.comp) = 1
;


------------------------------------------------------------------------------------------------------------------------

-- ALL GH PROFILE CITIES THAT ARE NOAA CITIES
REFRESH MATERIALIZED VIEW cities_in_profiles;
CREATE MATERIALIZED VIEW cities_in_profiles
AS
SELECT DISTINCT ON (nct.id) nct.id AS n_id,
                            addr.profile_location,
                            addr.comp,
                            addr.shortname,
                            nct.city_name AS n_name
FROM ( SELECT l.location AS profile_location,
              l.data #> '{0,address_components}'::text[] AS comp,
              btrim((l.data #> '{0,address_components,0,short_name}'::text[])::text, '"'::text) AS shortname
       FROM locations l) addr
         JOIN ( SELECT nc.id,
                       substr(nc.name, 0, "position"(nc.name, ', '::text)) AS city_name,
                       nc.name
                FROM cities_typed_view nc) nct ON nct.city_name = addr.shortname
WHERE addr.comp @> '[{"types": ["administrative_area_level_1"]}]'::jsonb
  AND NOT addr.comp @> '[{"types": ["administrative_area_level_5"]}]'::jsonb
;


------------------------------------------------------------------------------------------------------------------------

-- GEOCODED EVENTS BY DAY GROUPED BY NOAA LOCATION

SELECT count(e.id) as count,
       u.location AS profile_location,
       co.shortname AS country,
       co.n_id AS country_nid,
       ci.n_name AS city,
       ci.n_id AS city_nid
FROM events e
         JOIN users u ON u.id::text = e.userid
         FULL JOIN countries_in_profiles co ON co.profile_location = u.location
         FULL JOIN cities_in_profiles ci ON ci.profile_location = u.location
WHERE (co.n_id IS NOT NULL OR ci.n_id IS NOT NULL)
AND createdat >= '2020-01-01' AND createdat < '2020-01-02'
GROUP BY country, city, country_nid, city_nid, u.location;

-- MEASUREMENTS NEXT TO COMMIT COUNT AND LOCATION

-- 58mio in total => 18k geolocated measurements :(

SELECT day, EXTRACT(DOW FROM day) weekday, datatype, value as measurement, location_weather.location_name, location_weather.count as commit_count, counter.count as location_count FROM location_weather
  LEFT JOIN (
    SELECT location_name, count(location_name) as count FROM location_weather
    GROUP BY location_name
) counter ON counter.location_name = location_weather.location_name
WHERE datatype = 'PRCP'
ORDER BY location_count DESC, location_weather.location_name ASC, measurement ASC;
