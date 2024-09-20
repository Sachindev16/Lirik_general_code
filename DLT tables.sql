-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE circuits_bronze_raw00
COMMENT "circuit"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/circuits/", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE drivers_bronze_raw00
COMMENT "drivers"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/drivers/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE constructors_bronze_raw00
COMMENT "contructor"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/constructors/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lap_times_bronze_raw00
COMMENT "lap_times"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/lap_times/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pit_stops_bronze_raw00
COMMENT "pit_stops"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/pit_stops/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE qualifying_bronze_raw00
COMMENT "qualifying"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/qualifying/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE races_bronze_raw00
COMMENT "races"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/races/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE results_bronze_raw00
COMMENT "results"
AS SELECT *,current_timestamp() as date_added FROM cloud_files("dbfs:/mnt/databricksmountdltarch/results/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE races_bronze_cleaned00;

APPLY CHANGES INTO
  live.races_bronze_cleaned00
FROM
  stream(LIVE.races_bronze_raw00)
KEYS
  (raceId)
ignore null updates 
apply as delete when raceId is null
sequence by date_added

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE circuits_bronze_cleaned00;

APPLY CHANGES INTO
  live.circuits_bronze_cleaned00
FROM
  stream(LIVE.circuits_bronze_raw00)
KEYS
  (circuitId)
ignore null updates 
apply as delete when circuitId  is null
sequence by date_added


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE drivers_bronze_cleaned00;

APPLY CHANGES INTO
  live.drivers_bronze_cleaned00
FROM
  stream(LIVE.drivers_bronze_raw00)
KEYS
  (driverId)
ignore null updates 
apply as delete when driverId  is null
sequence by date_added

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE constructors_bronze_cleaned00;

APPLY CHANGES INTO
  live.constructors_bronze_cleaned00
FROM
  stream(LIVE.constructors_bronze_raw00)a
KEYS
  (constructorId)
ignore null updates 
apply as delete when constructorId is null
sequence by date_added

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lap_times_bronze_cleaned00;

APPLY CHANGES INTO
  live.lap_times_bronze_cleaned00
FROM
  stream(LIVE.lap_times_bronze_raw00)
KEYS
  (raceId,driverId,lap)
ignore null updates 
apply as delete when raceID is null and driverId is null and lap is null
sequence by date_added

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pit_stops_bronze_cleaned00;

APPLY CHANGES INTO
  live.pit_stops_bronze_cleaned00
FROM
  stream(LIVE.pit_stops_bronze_raw00)
KEYS
  (raceId,driverId,lap)
ignore null updates 
apply as delete when raceID is null and driverId is null and lap is null
sequence by date_added

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE qualifying_bronze_cleaned00;

APPLY CHANGES INTO
  live.qualifying_bronze_cleaned00
FROM
  stream(LIVE.qualifying_bronze_raw00)
KEYS
  (qualifyId)
ignore null updates 
apply as delete when qualifyID is null 
sequence by date_added


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE results_bronze_cleaned00;

APPLY CHANGES INTO
  live.results_bronze_cleaned00
FROM
  stream(LIVE.results_bronze_raw00)
KEYS
  (resultId)
ignore null updates 
apply as delete when resultID is null 
sequence by date_added 


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE races_silver00 as 
select raceId as race_id, year as race_year, round, circuitId as circuit_id,name, concat(date,time) as race_timestamp,date_added from stream(Live.races_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE circuits_silver00 as 
select circuitId as circuit_id,circuitRef as circuit_ref,name,location,country,lat as latitude,lng as longitude,alt as altitude, date_added from stream(Live.circuits_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE drivers_silver00 as 
select driverId as driver_id, driverRef as driver_ref, concat(forename,surname) as name, nationality, date_added from stream(Live.drivers_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE constructors_silver00 as 
select constructorId as constructor_id, constructorRef as constructor_ref, name as manufacturer, nationality, date_added from stream(Live.constructors_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lap_times_silver00 as 
select driverId as driver_id, raceId as race_id, lap, position,date_added from stream(Live.lap_times_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pit_stops_silver00 as 
select raceId as race_id, driverId as driver_id, stop, lap, time, duration, date_added from stream(Live.pit_stops_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE qualifying_silver00 as 
select qualifyId as qualify_id, raceId as race_id, driverId as driver_id, constructorId as constructor_id, position, date_added from stream(Live.qualifying_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE results_silver00 as 
select resultId as result_id, raceId as race_id, driverId as driver_id, constructorId as constructor_id, position, points,laps, date_added from stream(Live.results_bronze_cleaned00);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE drivers_standings_yearly_gold08 as 
select r.race_year as race_year, d.name as driver_name, sum(res.points) as total_points from stream(live.races_silver00) r inner join live.results_silver00 res on r.race_id=res.race_id inner join live.drivers_silver00 d on res.driver_id = d.driver_id  group by race_year, driver_name;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE drivers_standings_gold09 as
select d.name as driver_name,sum(r.points)as total_points,sum(case when r.position=1 then 1 else 0 end) as wins
from stream(live.drivers_silver00) d   inner join live.results_silver00 r on d.driver_id=r.driver_id  group by driver_name;


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE constructor_standings_gold08 as 
select c.manufacturer as team, sum(r.points)as total_points,sum(case when r.position=1 then 1 else 0 end) as wins
from stream(live.drivers_silver00) d   inner join live.results_silver00 r on d.driver_id=r.driver_id inner join live.constructors_silver00 c on r.constructor_id=c.constructor_id group by c.manufacturer;


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE less_pit_stops_gold01 as
select driver_id ,round(avg(stop),3) as less_pit_stops from stream(live.pit_stops_silver00)
group by driver_id ;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE races_having_most_pit_stops_gold02 AS
select race_id ,count(stop) as most_pit_stops from stream(live.pit_stops_silver00)
group by race_id ;

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW  abc as (select driver_name, circuit, total_points from (select driver_name,circuit,total_points, rank() over(PARTITION BY driver_name order by total_points desc) as rnk from dlt.favourite_circuits_raw_gold10) where rnk=1);

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE favourite_circuits_raw_gold11 AS
select * from live.abc;

-- COMMAND ----------


