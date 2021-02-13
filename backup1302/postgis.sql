-- table creation

CREATE TABLE points
(timestamp timestamp, mmsi char(9), latitude float, longitude float);

-- csv import
-- Juiste file permission aanzetten > https://ourtechroom.com/tech/importing-csv-file-in-postgresql-table/
COPY points FROM 'D:/Thesis/Data/aisdk_20201021_processed.csv' DELIMITER ',' CSV;

-- bounding box filtering
DELETE FROM points
WHERE (latitude NOT BETWEEN 55.897373 AND 57.834833) OR (longitude NOT BETWEEN 10.216449 AND 13.104204);

-- timestamp conversion
ALTER TABLE points
ADD COLUMN epoch integer;

UPDATE points
SET epoch = (extract(EPOCH from timestamp));

-- point geometry conversion
SELECT AddGeometryColumn ('public','points','geom',4326,'POINT',2);

UPDATE points
SET geom = (SELECT ST_SetSRID((ST_MakePoint(longitude, latitude)), 4326));
	
-- points table index creation
CREATE INDEX idx_mmsi on points(mmsi);
CREATE INDEX idx_epoch on points(epoch);
CREATE INDEX idx_geom on points USING GIST (geom);

-- segments table creation
CREATE TABLE segments
(mmsi char(9), epoch_start integer, epoch_end integer);

SELECT AddGeometryColumn ('public','segments','geom',4326,'LINESTRING',2);

-- segments creation
WITH segments AS (
	SELECT
		mmsi,
		lag(epoch, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS epoch_start,
		epoch AS epoch_end,
		ST_MakeLine(lag(geom, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch), geom) AS geom
	FROM
		points)
INSERT INTO segments SELECT * FROM segments WHERE geom IS NOT null;


-- segments table creation 2
CREATE TABLE segments
(mmsi char(9), epoch_start integer, epoch_end integer);

SELECT AddGeometryColumn ('public','segments','geom_start',4326,'POINT',2);
SELECT AddGeometryColumn ('public','segments','geom_end',4326,'POINT',2);

-- segments creation 2
WITH i AS (
	SELECT
		mmsi,
		lag(epoch, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS epoch_start,
		epoch AS epoch_end,
		lag(geom, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS geom_start,
		geom AS geom_end
	FROM
		points)
INSERT INTO segments SELECT * FROM i WHERE epoch_start IS NOT NULL AND geom_start IS NOT NULL;

-- 3D indexing 2A
SELECT AddGeometryColumn ('public','segments','geom3d',4326,'GEOMETRY',3); -- hoeft niet per se volumetric te zijn > als een schip op dezelfde plek blijft is de extent een linestringZ

UPDATE segments
SET geom3d = ST_SetSRID(ST_3DMakeBox(
	ST_MakePoint(ST_X(geom_start),ST_Y(geom_start),epoch_start),
	ST_MakePoint(ST_X(geom_end),ST_Y(geom_end),epoch_end)
	), 4326);

SELECT
    a.mmsi,
	b.mmsi,
	a.epoch_start,
	b.epoch_end
FROM
    segments as a,
    segments as b
WHERE
    st_3Dintersects(a.geom3d, b.geom3d);

-- not supported for polyhydral
UPDATE segments
SET geom3d = ST_Extrude(geom3d,0.0005, 0.0005, 5); -- ongeveer 50meter, 5 seconden

-- 3D indexing 2B final
SELECT AddGeometryColumn ('public','segments','geom3d',4326,'LINESTRINGZ',3)

UPDATE segments
SET geom3d = ST_SetSRID(ST_MakeLine(
	ST_MakePoint(ST_X(geom_start),ST_Y(geom_start),epoch_start),
	ST_MakePoint(ST_X(geom_end),ST_Y(geom_end),epoch_end)
	), 4326);

CREATE INDEX idx_segments_geom3d on segments USING GIST (geom3d gist_geometry_ops_nd);

SELECT ST_AsText(geom3d),
	   ST_AsText(ST_MakeLine(
			ST_MakePoint(ST_XMin(geom3d)-0.0005, ST_YMin(geom3d)-0.0005, ST_ZMin(geom3d)), 
			ST_MakePoint(ST_XMax(geom3d)+0.0005, ST_YMax(geom3d)+0.0005, ST_ZMax(geom3d))
		)) FROM segments WHERE mmsi = '241372000' OR mmsi = '219000368';


SELECT
    a.mmsi,
	b.mmsi,
	ST_Force2D(a.geom3d),
	ST_Force2D(b.geom3d)
FROM
    segments as a,
    segments as b
WHERE
			a.geom3d &&&
			ST_SetSRID(ST_MakeLine(
				ST_MakePoint(ST_XMin(b.geom3d)-0.01, ST_YMin(b.geom3d)-0.01, ST_ZMin(b.geom3d)), 
				ST_MakePoint(ST_XMax(b.geom3d)+0.01, ST_YMax(b.geom3d)+0.01, ST_ZMax(b.geom3d))
		), 4326)
    AND a.mmsi <> b.mmsi LIMIT 5;
	

-- segments table index creation
-- 2D indexing
CREATE INDEX idx_segments_mmsi on segments(mmsi);
CREATE INDEX idx_segments_epoch_start on segments(epoch_start);
CREATE INDEX idx_segments_epoch_end on segments(epoch_end);
CREATE INDEX idx_segments_geom on segments USING GIST (geom);

-- 3D indexing A
CREATE INDEX idx_segments_mmsi on segments(mmsi);
CREATE INDEX idx_segments_epoch_start on segments(epoch_start);
CREATE INDEX idx_segments_epoch_end on segments(epoch_end);
CREATE INDEX idx_segments_geom on segments USING GIST (geom gist_geometry_ops_nd);

-- 3D indexing B
CREATE TABLE segments3d AS SELECT * FROM segments;

SELECT AddGeometryColumn ('public','segments3d','geom3d',4326,'LINESTRINGM',3);

UPDATE segments3d
SET geom3d = (SELECT ST_SetSRID((ST_AddMeasure(geom, epoch_start, epoch_end)), 4326));

CREATE INDEX idx_segments3d_mmsi on segments3d(mmsi);
CREATE INDEX idx_segments3d_geom3d on segments3d USING GIST (geom3d gist_geometry_ops_nd);

EXPLAIN SELECT
    ST_3DIntersection(a.geom3d, b.geom3d),
    a.mmsi,
	b.mmsi,
	a.geom,
	b.geom
FROM
    segments3d as a,
    segments3d as b
WHERE
    st_3Dintersects(a.geom3d, b.geom3d)
    and a.mmsi = '241372000'
    AND b.mmsi = '219000368';

-- 3D indexing C
SELECT AddGeometryColumn ('public','segments3d','geom3d',4326,'LINESTRINGM',3);


SELECT ST_3DMakeBox(
	ST_MakePoint(ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), epoch_start),
	ST_MakePoint(ST_X(ST_EndPoint(geom)), ST_Y(ST_StartPoint(geom)), epoch_end)
	)
	FROM segments3d WHERE (mmsi = '241372000' OR mmsi = '219000368') AND epoch_start between 1603251100 and 1603251960;


-- get segments intersections

SELECT
    ST_Intersection(a.geom, b.geom),
    a.mmsi,
	b.mmsi,
	a.geom,
	b.geom
FROM
    segments as a,
    segments as b
WHERE
    st_intersects(a.geom, b.geom)
    and a.mmsi = '241372000'
    AND b.mmsi = '219000368';

-- vessels table creation
CREATE TABLE vessels
(mmsi char(9), type_of_mobile char(7), callsign char(7), name text, ship_type text);

-- test queries

http://blog.cleverelephant.ca/2015/02/breaking-linestring-into-segments.html

    WITH segments AS (
    SELECT gid, ST_AsText(ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY gid ORDER BY gid, (pt).path), (pt).geom)) AS geom
      FROM (SELECT gid, ST_DumpPoints(geom) AS pt FROM lines) as dumps
    )
    SELECT * FROM segments WHERE geom IS NOT NULL;

WITH
	trajectories
AS (
SELECT
	mmsi,
	
	ST_AsText(ST_MakeLine(lag((geom), 1, NULL) OVER (PARTITION BY mmsi ORDER BY epoch, (geom).path), (geom))) AS traject
FROM
	points
WHERE
	mmsi = '241372000'
) 
SELECT traject FROM trajectories

INSERT INTO
	trajectories(mmsi)
SELECT DISTINCT mmsi FROM points LIMIT 20;

UPDATE
	trajectories
SET
	geom = 
	(SELECT ST_MakeLine(points.geom ORDER BY points.epoch) FROM points WHERE trajectories.mmsi = points.mmsi);


CREATE TABLE test AS SELECT * FROM points where mmsi = '241372000' ORDER BY epoch asc;
ALTER TABLE test ADD COLUMN gid SERIAL;

SELECT 
	a.mmsi, ST_MakeLine(ARRAY[a.geom, b.geom]) AS geom, a.vert, b.vert 
FROM
	test a, test b 
WHERE
	a.mmsi = b.mmsi AND a.vert = b.vert-1 AND b.vert > 1;

CREATE TABLE segments AS (
WITH segments AS (
SELECT
	mmsi,
	lag(epoch, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch) AS epoch_start,
	epoch AS epoch_end,
	ST_MakeLine(lag(geom, 1, NULL) OVER (PARTITION BY mmsi ORDER BY mmsi, epoch), geom) AS segment_geom
FROM
	points)
SELECT * FROM segments WHERE segment_geom IS NOT null;

-- test queries 3D

CREATE EXTENSION postgis_sfcgal;

SELECT
	 *, ST_3DIntersection(ferry.geom, tanker.geom)
FROM trajectories AS ferry, trajectories AS tanker
WHERE ferry.mmsi = '241372000' AND tanker.mmsi = '219000368';

SELECT * FROM points where (mmsi = '241372000' OR mmsi = '219000368') AND epoch between 1603251100 and 1603251960;


-- filter segments
DELETE FROM segments 
WHERE ST_Length(geom3d) > 0.01;