--- Maritime risk analysis analysis ---

-- points table creation
CREATE TABLE points
(timestamp TIMESTAMP, mmsi CHAR(9), latitude FLOAT, longitude FLOAT, rot FLOAT, sog FLOAT, cog FLOAT, ship_type CHAR(30));

-- csv import
-- set correct file permission > https://ourtechroom.com/tech/importing-csv-file-in-postgresql-table/
COPY points FROM '<file>' DELIMITER ',' CSV;

-- timestamp conversion
ALTER TABLE points ADD COLUMN epoch INTEGER;
UPDATE points SET epoch = (EXTRACT(EPOCH FROM timestamp));

-- centripetal acceleration calculation
ALTER TABLE points ADD COLUMN ca FLOAT;
UPDATE points SET ca = (rot * sog);

-- point geometry conversion
SELECT AddGeometryColumn ('public','points','geom',4326,'POINT',2);
UPDATE points SET geom = (SELECT ST_SetSRID((ST_MakePoint(longitude, latitude)), 4326));
	
-- points table index creation
CREATE INDEX idx_points_mmsi ON points(mmsi);
CREATE INDEX idx_points_epoch ON points(epoch);
CREATE INDEX idx_points_geom ON points USING GIST (geom);

-- segments table creation
CREATE TABLE segments (mmsi CHAR(9), epoch_start INTEGER, epoch_end INTEGER, ca FLOAT, sog FLOAT, cog FLOAT);
SELECT AddGeometryColumn ('public','segments','geom_start',4326,'POINT',2);
SELECT AddGeometryColumn ('public','segments','geom_end',4326,'POINT',2);

-- segments creation
WITH cte AS (
	SELECT
		mmsi,
		LAG(epoch, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS epoch_start,
		epoch AS epoch_end,
		GREATEST(ABS(LAG(ca, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch)), ABS(ca)) as ca,
		GREATEST(ABS(LAG(sog, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch)), ABS(sog)) as sog,
		(ABS(LAG(cog, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch)) + ABS(cog)) / 2 as cog,
		LAG(geom, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS geom_start,
		geom AS geom_end
	FROM points
	)
INSERT INTO segments SELECT * FROM cte WHERE (epoch_start IS NOT NULL) AND (geom_start IS NOT NULL);

-- transform segments to 3D linestring model
SELECT AddGeometryColumn ('public','segments','geom3d',4326,'LINESTRINGZ',3);
UPDATE segments SET geom3d = ST_SetSRID(ST_MakeLine(
	ST_MakePoint(ST_X(geom_start), ST_Y(geom_start), epoch_start),
	ST_MakePoint(ST_X(geom_end), ST_Y(geom_end), epoch_end)
	), 4326);
	
-- remove unrealistic segments
DELETE FROM segments WHERE (ST_Length(ST_Transform(geom3d, 25832)) > 1000) OR (epoch_end - epoch_start > 500);

-- segments table index creation
CREATE INDEX idx_segments_geom3d ON segments USING GIST (geom3d gist_geometry_ops_nd);
CREATE INDEX idx_segments_mmsi ON segments(mmsi);

-- encounter segments table creation
CREATE TABLE encounter_segments (mmsi_a CHAR(9), mmsi_b CHAR(9), epoch_start INTEGER, epoch_end INTEGER);

-- selecting encounter segments using 3D intersection (lat,lon,time)
INSERT INTO encounter_segments
SELECT
    a.mmsi AS mmsi_a,
	b.mmsi AS mmsi_b,
	LEAST(a.epoch_start, b.epoch_start) AS epoch_start,
	GREATEST(a.epoch_end, b.epoch_end) AS epoch_end
FROM
    segments as a, segments as b
WHERE
	a.geom3d &&& ST_Transform(ST_SetSRID(ST_MakeLine(
		ST_MakePoint(ST_XMin(ST_Transform(b.geom3d, 25832)) - 500, ST_YMin(ST_Transform(b.geom3d, 25832)) - 500, ST_ZMin(b.geom3d)), 
		ST_MakePoint(ST_XMax(ST_Transform(b.geom3d, 25832)) + 500, ST_YMax(ST_Transform(b.geom3d, 25832)) + 500, ST_ZMax(b.geom3d))
		), 25832), 4326)
    AND a.mmsi <> b.mmsi;

-- removing duplicate encounters segments (shipA/shipB shipB/shipA)
DELETE FROM encounter_segments WHERE mmsi_a > mmsi_b
	AND EXISTS (
		SELECT * FROM encounter_segments AS lookup
		WHERE lookup.mmsi_a = encounter_segments.mmsi_b AND lookup.mmsi_b = encounter_segments.mmsi_a
        );

-- grouping encounters segments into higher level unique encounters
CREATE INDEX idx_encounter_segments_mmsi_a_mmsi_b ON encounter_segments(mmsi_a, mmsi_b) INCLUDE (epoch_start, epoch_end);

ALTER TABLE encounter_segments ADD COLUMN encounter_id INTEGER;

WITH cte AS (SELECT mmsi_a, mmsi_b, epoch_start, epoch_end, DENSE_RANK() over (ORDER BY mmsi_a, mmsi_b) AS encounter_id FROM encounter_segments)
UPDATE encounter_segments
SET encounter_id = cte.encounter_id FROM cte WHERE encounter_segments.mmsi_a = cte.mmsi_a AND encounter_segments.mmsi_b = cte.mmsi_b;

-- encounters table creation
CREATE TABLE encounters (encounter_id INTEGER, mmsi_a CHAR(9), mmsi_b CHAR(9), epoch_start INTEGER, epoch_end INTEGER);

-- merging grouped encounter segments to higher level unique encounters
INSERT INTO encounters SELECT DISTINCT
	encounter_id,
	mmsi_a,
	mmsi_b,
	MIN(epoch_start) OVER (PARTITION BY encounter_id) AS epoch_start,
	MAX(epoch_end) OVER (PARTITION BY encounter_id) AS epoch_end
FROM encounter_segments;

-- collecting extended linestring geometry from segments table
SELECT AddGeometryColumn ('public','encounters','geom_a',4326,'LINESTRINGZ',3);
SELECT AddGeometryColumn ('public','encounters','geom_b',4326,'LINESTRINGZ',3);

WITH cte AS (SELECT mmsi, epoch_start, epoch_end, ST_MakeLine(geom3d ORDER BY epoch_start) AS geoms FROM segments GROUP BY mmsi, epoch_start, epoch_end)
UPDATE encounters
SET geom_a = (SELECT ST_MakeLine(cte.geoms ORDER BY cte.epoch_start) FROM cte
WHERE encounters.mmsi_a = cte.mmsi AND (encounters.epoch_start - 60) <= cte.epoch_start AND (encounters.epoch_end + 60) >= cte.epoch_end);

WITH cte AS (SELECT mmsi, epoch_start, epoch_end, ST_MakeLine(geom3d ORDER BY epoch_start) AS geoms FROM segments GROUP BY mmsi, epoch_start, epoch_end)
UPDATE encounters
SET geom_b = (SELECT ST_MakeLine(cte.geoms ORDER BY cte.epoch_start) FROM cte
WHERE encounters.mmsi_b = cte.mmsi AND (encounters.epoch_start - 60) <= cte.epoch_start AND (encounters.epoch_end + 60) >= cte.epoch_end);

-- calculate closest ship positions in time and space, and distance at that point
SELECT AddGeometryColumn ('public','encounters','closest_point_a',4326,'POINTZ',3);
SELECT AddGeometryColumn ('public','encounters','closest_point_b',4326,'POINTZ',3);
ALTER TABLE encounters ADD COLUMN shortest_distance FLOAT;

UPDATE encounters SET closest_point_a = ST_3DClosestPoint(geom_a, geom_b);
UPDATE encounters SET closest_point_b = ST_3DClosestPoint(geom_b, geom_a);
UPDATE encounters SET shortest_distance = ST_Length(ST_Transform(ST_MakeLine(closest_point_a, closest_point_b), 25832));

-- calculate relative course (encounter type)
ALTER TABLE encounters
ADD COLUMN cog_a FLOAT, ADD COLUMN cog_b FLOAT, ADD COLUMN cog_diff FLOAT;

WITH cte AS (SELECT mmsi, epoch_start, epoch_end, cog FROM segments)
UPDATE encounters
SET cog_a = (SELECT cog) FROM cte
WHERE encounters.mmsi_a = cte.mmsi
AND cte.epoch_start <= ST_Z(encounters.closest_point_a)
AND ST_Z(encounters.closest_point_a) <= cte.epoch_end;

WITH t AS (SELECT mmsi, epoch_start, epoch_end, cog FROM segments)
UPDATE encounters
SET cog_b = (SELECT cog) FROM t
WHERE encounters.mmsi_b = t.mmsi
AND t.epoch_start <= ST_Z(encounters.closest_point_b)
AND ST_Z(encounters.closest_point_b) <= t.epoch_end;

UPDATE encounters SET cog_diff = ABS(cog_a - cog_b);

-- calculate ship domain overlap
SELECT AddGeometryColumn ('public','encounters','ship_domain_a',4326,'POLYGON',2);
SELECT AddGeometryColumn ('public','encounters','ship_domain_b',4326,'POLYGON',2);
ALTER TABLE encounters ADD COLUMN ship_domain_overlap FLOAT;

UPDATE encounters
SET ship_domain_a = (SELECT ST_Transform(
	ST_Rotate(	
		ST_Scale(
			ST_Buffer(
				ST_Transform(closest_point_a, 25832)
			, 1000)
		,ST_Point(0.85, 1.65), ST_Transform(closest_point_a, 25832))
	, RADIANS(180-cog_a), ST_Transform(closest_point_a, 25832))
, 4326));

UPDATE encounters
SET ship_domain_b = (SELECT ST_Transform(
	ST_Rotate(	
		ST_Scale(
			ST_Buffer(
				ST_Transform(closest_point_b, 25832)
			, 1000)
		,ST_Point(0.85, 1.65), ST_Transform(closest_point_b, 25832))
	, RADIANS(180-cog_b), ST_Transform(closest_point_b, 25832))
, 4326));

UPDATE encounters
SET ship_domain_overlap = (SELECT ST_Area(ST_Transform(ST_INTERSECTION(ship_domain_a, ship_domain_b), 25832))
WHERE ST_Overlaps(ship_domain_a, ship_domain_b));

-- calculate speed difference
ALTER TABLE encounters ADD COLUMN max_sog_a FLOAT, ADD COLUMN max_sog_b FLOAT, ADD COLUMN max_sog_diff FLOAT;

WITH cte AS (SELECT mmsi, epoch_start, epoch_end, MAX(sog) AS sog FROM segments GROUP BY mmsi, epoch_start, epoch_end)
UPDATE encounters
SET max_sog_a = (SELECT MAX(sog) FROM cte
WHERE encounters.mmsi_a = cte.mmsi AND (encounters.epoch_start - 60) <= cte.epoch_start AND (encounters.epoch_end + 60) >= cte.epoch_end);

WITH cte AS (SELECT mmsi, epoch_start, epoch_end, MAX(sog) AS sog FROM segments GROUP BY mmsi, epoch_start, epoch_end)
UPDATE encounters
SET max_sog_b = (SELECT MAX(sog) FROM cte
WHERE encounters.mmsi_b = cte.mmsi AND (encounters.epoch_start - 60) <= cte.epoch_start AND (encounters.epoch_end + 60) >= cte.epoch_end);

UPDATE encounters
SET max_sog_diff = ABS(max_sog_a - max_sog_b);

-- calculate centripetal acceleration
ALTER TABLE encounters ADD COLUMN max_ca FLOAT;
WITH cte AS (SELECT mmsi, epoch_start, epoch_end, MAX(ca) AS ca FROM segments GROUP BY mmsi, epoch_start, epoch_end)
UPDATE encounters
SET max_ca = (SELECT MAX(ca) FROM cte
WHERE (encounters.mmsi_a = cte.mmsi OR encounters.mmsi_b = cte.mmsi) AND (encounters.epoch_start - 60) <= cte.epoch_start AND (encounters.epoch_end + 60) >= cte.epoch_end);

-- choose point based on 3D closest point that represents encounter
SELECT AddGeometryColumn ('public','encounters','encounter_centroid',4326,'POINT',2);
UPDATE encounters SET encounter_centroid = ST_Centroid(ST_Collect(closest_point_a, closest_point_b));

-- include ship types for visualisation purposes
ALTER TABLE encounters ADD COLUMN ship_type_a CHAR(30), ADD COLUMN ship_type_b CHAR(30);
UPDATE encounters SET ship_type_a = (SELECT MODE() WITHIN GROUP (ORDER BY ship_type) FROM points WHERE encounters.mmsi_a = points.mmsi);
UPDATE encounters SET ship_type_b = (SELECT MODE() WITHIN GROUP (ORDER BY ship_type) FROM points WHERE encounters.mmsi_b = points.mmsi);
