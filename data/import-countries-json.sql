DROP TABLE IF EXISTS n_countries_json CASCADE;
CREATE TABLE n_countries_json(jsonrecord json);

COPY n_countries_json FROM '/tmp/input/countries.json';

create view countries_typed_view as
select
    (n_countries_json.jsonrecord ->> 'id')::text AS id,
    (n_countries_json.jsonrecord ->> 'mindate')::timestamp AS mindate,
    (n_countries_json.jsonrecord ->> 'maxdate')::timestamp AS maxdate,
    (n_countries_json.jsonrecord ->> 'name')::text AS name
from n_countries_json;

-- generic approach does not work because
-- do $$
-- declare
--     l_keys text;
-- begin
--     drop view if exists countries_view cascade;
-- --
--     select string_agg(distinct format('jsonrecord ->> %L as %I', jkey, jkey), ', ')
--     into l_keys
--     from n_countries_json, json_object_keys(jsonrecord) as t(jkey);
-- --
--     execute 'create view countries_view as select ' || l_keys || ' from n_countries_json';
-- --
--     execute 'select * from countries_view';
-- end$$;
