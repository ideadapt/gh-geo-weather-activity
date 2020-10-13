DROP TABLE IF EXISTS n_cities_json CASCADE;
CREATE TABLE n_cities_json(jsonrecord json);

COPY n_cities_json FROM '/tmp/input/cities.json';

create view cities_typed_view as
select
    (n_cities_json.jsonrecord ->> 'id')::text AS id,
    (n_cities_json.jsonrecord ->> 'mindate')::timestamp AS mindate,
    (n_cities_json.jsonrecord ->> 'maxdate')::timestamp AS maxdate,
    (n_cities_json.jsonrecord ->> 'name')::text AS name
from n_cities_json;

-- generic approach does not work because
-- do $$
-- declare
--     l_keys text;
-- begin
--     drop view if exists countries_view cascade;
-- --
--     select string_agg(distinct format('jsonrecord ->> %L as %I', jkey, jkey), ', ')
--     into l_keys
--     from n_cities_json, json_object_keys(jsonrecord) as t(jkey);
-- --
--     execute 'create view countries_view as select ' || l_keys || ' from n_cities_json';
-- --
--     execute 'select * from countries_view';
-- end$$;
