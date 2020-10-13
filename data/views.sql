-- ALL COUNTRIES IN GH PROFILES
REFRESH MATERIALIZED VIEW countries_in_profiles;
CREATE MATERIALIZED VIEW countries_in_profiles
AS
SELECT addr.profile_location,
       addr.comp,
       addr.shortname,
       nct.country_id AS n_shortname,
       nct.id AS n_id
FROM (SELECT l.location AS profile_location,
             l.data #> '{0,address_components}'::text[] AS comp,
             btrim((l.data #> '{0,address_components,0,short_name}'::text[])::text, '"'::text) AS shortname
      FROM locations l) addr
         JOIN (SELECT nc.id,
                      substr(nc.id, 6, 2) AS country_id,
                      nc.name
               FROM countries_typed_view nc) nct ON nct.country_id = addr.shortname

WHERE addr.comp @> '[{"types": ["country"]}]'::jsonb
  AND jsonb_array_length(addr.comp) = 1
;


------------------------------------------------------------------------------------------------------------------------

-- ALL CITIES IN GH PROFILES
REFRESH MATERIALIZED VIEW cities_in_profiles;
CREATE MATERIALIZED VIEW cities_in_profiles
AS
SELECT addr.profile_location,
       addr.comp,
       addr.shortname,
       nct.city_name AS n_name,
       nct.id AS n_id
FROM (SELECT l.location AS profile_location,
             l.data #> '{0,address_components}'::text[] AS comp,
             btrim((l.data #> '{0,address_components,0,short_name}'::text[])::text, '"'::text) AS shortname
      FROM locations l) addr
         JOIN (SELECT nc.id,
                      substr(nc.name, 0, "position"(nc.name, ', '::text)) AS city_name,
                      nc.name
               FROM cities_typed_view nc) nct ON nct.city_name = addr.shortname
WHERE addr.comp @> '[{"types": ["administrative_area_level_1"]}]'::jsonb
  AND jsonb_array_length(addr.comp) = 3
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW events_geocoded
AS
SELECT e.id,
       e.type,
       e.userid,
       e.createdat,
       u.location AS profile_location,
       co.shortname AS country,
       co.n_id AS country_nid,
       ci.n_name AS city,
       ci.n_id AS city_nid
FROM events e
         JOIN users u ON u.id::text = e.userid::text
         FULL JOIN countries_in_profiles co ON co.profile_location = u.location
         FULL JOIN cities_in_profiles ci ON ci.profile_location = u.location
WHERE co.n_id IS NOT NULL OR ci.n_id IS NOT NULL
;
