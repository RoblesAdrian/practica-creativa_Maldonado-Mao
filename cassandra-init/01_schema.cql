CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin text,
  dest text,
  distance int,
  PRIMARY KEY ((origin), dest)
);
