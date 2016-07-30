CREATE TABLE revstore (
  appid text,
  dbname text,
  id text,
  rev text,
  global_seq serial,
  last_update timestamp,
  PRIMARY KEY(appid, dbname, id, rev)
);

CREATE TABLE checkpoints (
  appid text,
  dbname text,
  seq int,
  timestamp timestamp DEFAULT now(),
  PRIMARY KEY(appid, dbname, seq)
);

CREATE TABLE localdocs (
  appid text,
  dbname text,
  id text,
  rev text,
  doc jsonb,
  PRIMARY KEY(appid, dbname, id, rev, doc)
);

CREATE TABLE app_dbs (
  appid text,
  dbname text,
  endpoint text,
  PRIMARY KEY(appid, dbname)
);
