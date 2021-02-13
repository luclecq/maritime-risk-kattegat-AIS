-- points table creation
CREATE TABLE points2
(timestamp TIMESTAMP, mmsi CHAR(9), latitude FLOAT, longitude FLOAT;

-- csv import
-- Juiste file permission aanzetten > https://ourtechroom.com/tech/importing-csv-file-in-postgresql-table/
COPY points2 FROM 'D:/Thesis/Data/aisdk_20210121_processed.csv' DELIMITER ',' CSV;

-- timestamp conversion
ALTER TABLE points2 ADD COLUMN epoch INTEGER;

UPDATE points2 SET epoch = (EXTRACT(EPOCH FROM timestamp));

-- point geometry conversion
SELECT AddGeometryColumn ('public','points2','geom',4326,'POINT',2);

UPDATE points2 SET geom = (SELECT ST_SetSRID((ST_MakePoint(longitude, latitude)), 4326));
	
-- points table index creation
CREATE INDEX idx_mmsi2 ON points2(mmsi);
CREATE INDEX idx_epoch2 ON points2(epoch);
CREATE INDEX idx_geom2 ON points2 USING GIST (geom);

-- segments table creation
CREATE TABLE segments2 (mmsi CHAR(9), epoch_start INTEGER, epoch_end INTEGER);

SELECT AddGeometryColumn ('public','segments2','geom_start',4326,'POINT',2);
SELECT AddGeometryColumn ('public','segments2','geom_end',4326,'POINT',2);

-- segments creation
WITH i AS (
	SELECT
		mmsi,
		LAG(epoch, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS epoch_start,
		epoch AS epoch_end,
		LAG(geom, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS geom_start,
		geom AS geom_end
	FROM points2
	)
INSERT INTO segments2 SELECT * FROM i WHERE (epoch_start IS NOT NULL) AND (geom_start IS NOT NULL);

-- transform segments to 3D linestring model
SELECT AddGeometryColumn ('public','segments2','geom3d',4326,'LINESTRINGZ',3)

UPDATE segments2 SET geom3d = ST_SetSRID(ST_MakeLine(
	ST_MakePoint(ST_X(geom_start), ST_Y(geom_start), epoch_start),
	ST_MakePoint(ST_X(geom_end), ST_Y(geom_end), epoch_end)
	), 4326);
	
-- remove unrealistic segments
DELETE FROM segments2 WHERE (ST_Length(ST_Transform(geom3d, 25832)) > 1000) OR (epoch_end - epoch_start > 500);

-- segments table index creation
CREATE INDEX idx_segments_geom3d ON segments USING GIST (geom3d gist_geometry_ops_nd);

-- candidate segments table creation
CREATE TABLE candidate_segments2 (mmsi_a CHAR(9), mmsi_b CHAR(9), epoch_start INTEGER, epoch_end INTEGER);

-- selecting candidate segments using 3D intersection (lat,lon,time)
INSERT INTO candidate_segments2
SELECT
    a.mmsi AS mmsi_a,
	b.mmsi AS mmsi_b,
	LEAST(a.epoch_start, b.epoch_start) AS epoch_start,
	GREATEST(a.epoch_end, b.epoch_end) AS epoch_end
FROM
    segments2 as a, segments2 as b
WHERE
	a.geom3d &&& ST_Transform(ST_SetSRID(ST_MakeLine(
		ST_MakePoint(ST_XMin(ST_Transform(b.geom3d, 25832)) - 500, ST_YMin(ST_Transform(b.geom3d, 25832)) - 500, ST_ZMin(b.geom3d)), 
		ST_MakePoint(ST_XMax(ST_Transform(b.geom3d, 25832)) + 500, ST_YMax(ST_Transform(b.geom3d, 25832)) + 500, ST_ZMax(b.geom3d))
		), 25832), 4326)
    AND a.mmsi <> b.mmsi;

-- removing duplicate candidates segments (shipA/shipB shipB/shipA)
DELETE FROM candidate_segments2 WHERE mmsi_a > mmsi_b
	AND EXISTS (
		SELECT * FROM candidate_segments2 AS lookup
		WHERE lookup.mmsi_a = candidate_segments2.mmsi_b AND lookup.mmsi_b = candidate_segments2.mmsi_a
        );

-- grouping candidates segments into higher level unique candidates
ALTER TABLE candidate_segments2
--ADD COLUMN next_epoch_start INTEGER,
ADD COLUMN candidate_id INTEGER;

--WITH t as (SELECT mmsi_a, mmsi_b, epoch_start, epoch_end,
--		   LEAD(epoch_start,1) OVER (ORDER BY mmsi_a, mmsi_b, epoch_start, epoch_end) as next_epoch_start
--  			from candidate_segments2)
--UPDATE candidate_segments2
--SET next_epoch_start = t.next_epoch_start
--FROM t
--WHERE candidate_segments2.mmsi_a = t.mmsi_a AND candidate_segments2.mmsi_b = t.mmsi_b AND candidate_segments2.epoch_start = t.epoch_start AND candidate_segments2.epoch_end = t.epoch_end;

WITH t AS (SELECT mmsi_a, mmsi_b, DENSE_RANK() over (ORDER BY mmsi_a, mmsi_b) AS candidate_id
FROM candidate_segments2 ORDER BY mmsi_a, mmsi_b)
UPDATE candidate_segments2 SET candidate_id = t.candidate_id FROM t
WHERE candidate_segments2.mmsi_a = t.mmsi_a AND candidate_segments2.mmsi_b = t.mmsi_b;

-- candidates table creation
CREATE TABLE candidates2 (candidate_id INTEGER, mmsi_a CHAR(9), mmsi_b CHAR(9), epoch_start INTEGER, epoch_end INTEGER);

-- merging grouped candidate segments to higher level unique candidates
INSERT INTO candidates2
SELECT DISTINCT
candidate_id,
mmsi_a,
mmsi_b,
MIN(epoch_start) OVER (PARTITION BY candidate_id) AS epoch_start,
MAX(epoch_end) OVER (PARTITION BY candidate_id) AS epoch_end
FROM candidate_segments2;

-- get extended linestring geometry that corresponds with the higher level unique candidates
SELECT AddGeometryColumn ('public','candidates2','geom_a',4326,'LINESTRINGZ',3);
SELECT AddGeometryColumn ('public','candidates2','geom_b',4326,'LINESTRINGZ',3);

WITH subquery AS (SELECT mmsi, epoch_start, epoch_end, ST_MakeLine(geom3d ORDER BY epoch_start) AS geoms FROM segments2 GROUP BY mmsi, epoch_start, epoch_end)
UPDATE candidates2
SET geom_a = (SELECT ST_MakeLine(subquery.geoms ORDER BY subquery.epoch_start) FROM subquery
WHERE candidates2.mmsi_a = subquery.mmsi AND (candidates2.epoch_start - 60) <= subquery.epoch_start AND (candidates2.epoch_end + 60) >= subquery.epoch_end);

WITH subquery AS (SELECT mmsi, epoch_start, epoch_end, ST_MakeLine(geom3d ORDER BY epoch_start) AS geoms FROM segments2 GROUP BY mmsi, epoch_start, epoch_end)
UPDATE candidates2
SET geom_b = (SELECT ST_MakeLine(subquery.geoms ORDER BY subquery.epoch_start) FROM subquery
WHERE candidates2.mmsi_b = subquery.mmsi AND (candidates2.epoch_start - 60) <= subquery.epoch_start AND (candidates2.epoch_end + 60) >= subquery.epoch_end);

-- calculate closest point in time and space and distance at that point
SELECT AddGeometryColumn ('public','candidates2','closest_line',4326,'LINESTRINGZ',3);
ALTER TABLE candidates2 ADD COLUMN closest_line_distance INTEGER;
SELECT AddGeometryColumn ('public','candidates2','closest_line_centroid',4326,'POINTZ',3);


UPDATE candidates2 SET closest_line = ST_3DShortestLine(geom_a, geom_b);
UPDATE candidates2 SET closest_line_distance = ST_3DLength(ST_Transform(closest_line, 25832)
UPDATE candidates2 SET closest_line_centroid = ST_Centroid(closest_line);

--
-- vessels table creation
CREATE TABLE vessels
(mmsi char(9), type_of_mobile char(7), callsign char(7), name text, ship_type text);

