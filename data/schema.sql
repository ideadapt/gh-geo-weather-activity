CREATE TABLE events
(
    id SERIAL,
    type VARCHAR(50),
    repoid VARCHAR(100),
    reponame  VARCHAR(250),
    userid   VARCHAR(100),
    username VARCHAR(250),
    repolanguage VARCHAR(50),
    forkid VARCHAR(100),
    forkname VARCHAR(250),
    createdat TIMESTAMP,
    PRIMARY KEY (id)
);
CREATE INDEX events_type_index ON events(type);
CREATE INDEX events_userID_index ON events(userid);

create table location_weather(
    id int generated always as identity,
    datatype varchar(10),
    day timestamp without time zone,
    value numeric(7, 2),
    location_name varchar(100),
    location_id varchar(20),
    PRIMARY KEY(id),
    CONSTRAINT location_day UNIQUE (day, location_id, datatype)
);


CREATE TABLE repos
(
  id bigint PRIMARY KEY,
  name varchar(200) NOT NULL,
  language varchar(50),
  description text,
  license text,
  homepage text,
  size integer NOT NULL default 0,
  stars integer NOT NULL default 0,
  forks integer NOT NULL default 0,
  topics text[],
  deleted boolean NOT NULL default false,
  parentid integer,
  ownerid integer,
  created timestamp without time zone,
  modified timestamp without time zone,
  fetched timestamp without time zone,
  statuscode integer
);

CREATE TABLE users
(
  id integer PRIMARY KEY,
  login varchar(200) NOT NULL,
  name text,
  company text,
  location text,
  bio text,
  email text,
  blog text,
  type text,
  followers integer,
  following integer,
  created timestamp without time zone,
  modified timestamp without time zone,
  fetched timestamp without time zone,
  statuscode integer
);

CREATE TABLE organization_members
(
  organization integer PRIMARY KEY,
  members integer[],
  fetched timestamp without time zone,
  statuscode integer
);

CREATE TABLE locations
(
  location text PRIMARY KEY,
  fetched timestamp without time zone,
  data jsonb,
  city text,
  state text,
  country text
);



CREATE INDEX repos_name_index ON repos (name);
CREATE INDEX users_login_index ON users(login);

/* migrations: TODO: proper up/down
alter table repos add column license text;
alter table repos add column homepage text;
alter table users add column blog text;
*/
