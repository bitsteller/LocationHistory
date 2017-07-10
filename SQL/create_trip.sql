DROP TABLE IF EXISTS %(trip)t CASCADE;

DROP SEQUENCE IF EXISTS %(trip)t_id_seq;

CREATE SEQUENCE %(trip)t_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1480
  CACHE 1;

CREATE TABLE %(trip)t
(
  id integer NOT NULL DEFAULT nextval('%(trip)t_id_seq'::regclass),
  user_id character varying,
  start_geom geometry(Point,4326),
  end_geom geometry(Point,4326),
  start_time timestamp without time zone,
  end_time timestamp without time zone,
  distance double precision,
  geom geometry(LineString,4326),
  activity character varying,
  CONSTRAINT %(trip)t_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

COMMENT ON TABLE %(trip)t IS 
'Trip table containing GPS based trips. Activities include Driving, On a bus, On a train, On the subway, Walking.

%m';

CREATE INDEX %(trip)t_user_id_idx
  ON %(trip)t
  USING btree
  (user_id);

CREATE INDEX %(trip)t_start_time_idx
  ON %(trip)t
  USING btree
  (start_time);

CREATE INDEX %(trip)t_end_time_idx
  ON %(trip)t
  USING btree
  (end_time);