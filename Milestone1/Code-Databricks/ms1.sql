-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Milestone 1: Project Proposal & Data Selection/Preparation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## I. Import and clean data if necessary

-- COMMAND ----------

-- Create specific database if needed 
--CREATE DATABASE IF NOT EXISTS Sportstats

-- COMMAND ----------

-- Use default database instead 
USE default;

-- COMMAND ----------

-- Show existing tables
SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. athlete_events table

-- COMMAND ----------

-- View athlete_events data
SELECT * 
FROM athlete_events 

-- COMMAND ----------

SELECT DISTINCT City 
FROM athlete_events 

-- COMMAND ----------

DESCRIBE athlete_events; 

-- COMMAND ----------

-- Total number of observations
SELECT COUNT(*) as count
FROM athlete_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. noc_regions table 

-- COMMAND ----------

-- View noc_regions data 
SELECT * FROM noc_regions

-- COMMAND ----------

-- Total number of observations
SELECT COUNT(*) as count
FROM noc_regions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. host_cities table

-- COMMAND ----------

SELECT * 
FROM host_cities; 

-- COMMAND ----------

DESCRIBE host_cities; 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/host_countries", true)

-- COMMAND ----------

-- write host_cities without other unnecessary columns to Delta Silver 
DROP TABLE IF EXISTS host_countries; 

CREATE TABLE host_countries
USING DELTA
AS (
  SELECT DISTINCT City, Country
  FROM host_cities
  WHERE Country IS NOT NULL
) 

-- COMMAND ----------

SELECT *
FROM host_countries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## II. Aggregate tables 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/joineddata", true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Join 3 tables 

-- COMMAND ----------

-- join tables  
DROP TABLE IF EXISTS joinedData; 

CREATE TABLE joinedData
USING DELTA
/*USING CSV
OPTIONS(
  header "true", 
  inferSchema "true"
)*/
AS (
  SELECT * /*CONCAT(ID, '_' ,NOC) AS ID_NOC*/ 
  FROM athlete_events 
  LEFT JOIN noc_regions 
  USING(NOC)
  LEFT JOIN host_countries
  USING(City)
) 

-- COMMAND ----------

-- Total number of observations
SELECT COUNT(*) as count
FROM joinedData

-- COMMAND ----------

SELECT  * 
FROM joinedData 

-- COMMAND ----------

DESCRIBE joinedData; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reorder columns 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/final_joined", true)

-- COMMAND ----------

-- reoroder columns in joined table and change some entries in the Region and Country column 
DROP TABLE IF EXISTS final_joined; 

CREATE TABLE final_joined
USING DELTA
AS (
  SELECT 
    ID, 
    Name, 
    Sex, 
    Age, 
    Height, 
    Weight, 
    Team, 
    NOC, 
    (CASE WHEN NOC = 'SGP' THEN 'Singapore' 
          WHEN region = 'USA' THEN 'United States'
          WHEN region = 'UK' THEN 'United Kingdom'
          WHEN Team = 'Tuvalu' THEN 'Tuvalu'
          ELSE region END) AS Region,  /*change to match country column*/
    Games, 
    Year, 
    Season, 
    Sport, 
    Event, 
    City, 
    (CASE WHEN Country = 'Yugoslavia' THEN 'Serbia'
          ELSE Country END) AS Country, /*Yugoslavia was renamed as Serbia*/
    Medal, 
    notes AS Notes 
  FROM joinedData 
) 

-- COMMAND ----------

-- Total number of observations
SELECT COUNT(*) as count
FROM final_joined

-- COMMAND ----------

DESCRIBE final_joined

-- COMMAND ----------

SELECT * 
FROM final_joined

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Duplicate rows 

-- COMMAND ----------

-- Determine duplicate rows in the data 
SELECT DISTINCT ID, Name, Sex, Age, Height, Weight, Team, NOC, Region, Games, Year, Season, Sport, Event, City, Country, Medal, Notes
FROM final_joined
GROUP BY ID, Name, Sex, Age, Height, Weight, Team, NOC, Region, Games, Year, Season, Sport, Event, City, Country, Medal, Notes
HAVING COUNT(ID) > 1

-- COMMAND ----------

SELECT * 
FROM final_joined
WHERE ID = 704

-- COMMAND ----------

SELECT * 
FROM final_joined
WHERE ID = 2449

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/final_joined_distinct", true)

-- COMMAND ----------

-- only select distinct rows from the final_joined table 
DROP TABLE IF EXISTS final_joined_distinct; 

CREATE TABLE final_joined_distinct
USING DELTA
AS (
    SELECT DISTINCT ID, Name, Sex, Age, Height, Weight, Team, NOC, Region, Games, Year, Season, Sport, Event, City, Country, Medal, Notes
    FROM final_joined
) 

-- COMMAND ----------

-- non-duplicate table 
SELECT *
FROM final_joined_distinct

-- COMMAND ----------

-- Total number of observations
SELECT COUNT(*) as count
FROM final_joined_distinct

-- COMMAND ----------

DESCRIBE final_joined_distinct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## III. Data Exploration

-- COMMAND ----------

-- set partition to 8 (change to improve performance if needed) 
SET spark.sql.shuffle.partitions=8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Null values

-- COMMAND ----------

-- count number of null values in each column in original table 
SELECT 
COUNT(*)-COUNT(ID) As ID, 
COUNT(*)-COUNT(Name) As Name, 
COUNT(*)-COUNT(Sex) As Sex, 
COUNT(*)-COUNT(Age) As Age, 
COUNT(*)-COUNT(Height) As Height, 
COUNT(*)-COUNT(Weight) As Weight, 
COUNT(*)-COUNT(Team) As Team,
COUNT(*)-COUNT(NOC) As NOC,
COUNT(*)-COUNT(Region) As Region,
COUNT(*)-COUNT(Games) As Games,
COUNT(*)-COUNT(Year) As Year,
COUNT(*)-COUNT(Season) As Season,
COUNT(*)-COUNT(Sport) As Sport,
COUNT(*)-COUNT(Event) As Event,
COUNT(*)-COUNT(City) As City,
COUNT(*)-COUNT(Country) As Country,
COUNT(*)-COUNT(Medal) As Medal,
COUNT(*)-COUNT(Notes) As Notes
FROM final_joined


-- COMMAND ----------

-- count number of null values in each column in non-duplicate table 
SELECT 
COUNT(*)-COUNT(ID) As ID, 
COUNT(*)-COUNT(Name) As Name, 
COUNT(*)-COUNT(Sex) As Sex, 
COUNT(*)-COUNT(Age) As Age, 
COUNT(*)-COUNT(Height) As Height, 
COUNT(*)-COUNT(Weight) As Weight, 
COUNT(*)-COUNT(Team) As Team,
COUNT(*)-COUNT(NOC) As NOC,
COUNT(*)-COUNT(Region) As Region,
COUNT(*)-COUNT(Games) As Games,
COUNT(*)-COUNT(Year) As Year,
COUNT(*)-COUNT(Season) As Season,
COUNT(*)-COUNT(Sport) As Sport,
COUNT(*)-COUNT(Event) As Event,
COUNT(*)-COUNT(City) As City,
COUNT(*)-COUNT(Country) As Country,
COUNT(*)-COUNT(Medal) As Medal,
COUNT(*)-COUNT(Notes) As Notes
FROM final_joined_distinct

-- COMMAND ----------

-- count number of empty values in each column 
SELECT 
(COUNT(1)-COUNT(nullif(ID,''))) As ID, 
(COUNT(1)-COUNT(nullif(Name,''))) As Name, 
(COUNT(1)-COUNT(nullif(Sex,''))) As Sex, 
(COUNT(1)-COUNT(nullif(Age,''))) As Age, 
(COUNT(1)-COUNT(nullif(Height,''))) As Height, 
(COUNT(1)-COUNT(nullif(Weight,''))) As Weight, 
(COUNT(1)-COUNT(nullif(Team,''))) As Team,
(COUNT(1)-COUNT(nullif(NOC,''))) As NOC,
(COUNT(1)-COUNT(nullif(Region,''))) As Region,
(COUNT(1)-COUNT(nullif(Games,''))) As Games,
(COUNT(1)-COUNT(nullif(Year,''))) As Year,
(COUNT(1)-COUNT(nullif(Season,''))) As Season,
(COUNT(1)-COUNT(nullif(Sport,''))) As Sport,
(COUNT(1)-COUNT(nullif(Event,''))) As Event,
(COUNT(1)-COUNT(nullif(City,''))) As City,
(COUNT(1)-COUNT(nullif(Country,''))) As Country,
(COUNT(1)-COUNT(nullif(Medal,''))) As Medal,
(COUNT(1)-COUNT(nullif(notes,''))) As notes
FROM final_joined

-- COMMAND ----------

-- count number of empty values in each column 
SELECT 
(COUNT(1)-COUNT(nullif(ID,''))) As ID, 
(COUNT(1)-COUNT(nullif(Name,''))) As Name, 
(COUNT(1)-COUNT(nullif(Sex,''))) As Sex, 
(COUNT(1)-COUNT(nullif(Age,''))) As Age, 
(COUNT(1)-COUNT(nullif(Height,''))) As Height, 
(COUNT(1)-COUNT(nullif(Weight,''))) As Weight, 
(COUNT(1)-COUNT(nullif(Team,''))) As Team,
(COUNT(1)-COUNT(nullif(NOC,''))) As NOC,
(COUNT(1)-COUNT(nullif(Region,''))) As Region,
(COUNT(1)-COUNT(nullif(Games,''))) As Games,
(COUNT(1)-COUNT(nullif(Year,''))) As Year,
(COUNT(1)-COUNT(nullif(Season,''))) As Season,
(COUNT(1)-COUNT(nullif(Sport,''))) As Sport,
(COUNT(1)-COUNT(nullif(Event,''))) As Event,
(COUNT(1)-COUNT(nullif(City,''))) As City,
(COUNT(1)-COUNT(nullif(Country,''))) As Country,
(COUNT(1)-COUNT(nullif(Medal,''))) As Medal,
(COUNT(1)-COUNT(nullif(notes,''))) As notes
FROM final_joined_distinct

-- COMMAND ----------

-- count number of empty values in each column 
SELECT 
(COUNT(1)-COUNT(nullif(ID,'NA'))) As ID, 
(COUNT(1)-COUNT(nullif(Name,'NA'))) As Name, 
(COUNT(1)-COUNT(nullif(Sex,'NA'))) As Sex, 
(COUNT(1)-COUNT(nullif(Age,'NA'))) As Age, 
(COUNT(1)-COUNT(nullif(Height,'NA'))) As Height, 
(COUNT(1)-COUNT(nullif(Weight,'NA'))) As Weight, 
(COUNT(1)-COUNT(nullif(Team,'NA'))) As Team,
(COUNT(1)-COUNT(nullif(NOC,'NA'))) As NOC,
(COUNT(1)-COUNT(nullif(Region,'NA'))) As Region,
(COUNT(1)-COUNT(nullif(Games,'NA'))) As Games,
(COUNT(1)-COUNT(nullif(Year,'NA'))) As Year,
(COUNT(1)-COUNT(nullif(Season,'NA'))) As Season,
(COUNT(1)-COUNT(nullif(Sport,'NA'))) As Sport,
(COUNT(1)-COUNT(nullif(Event,'NA'))) As Event,
(COUNT(1)-COUNT(nullif(City,'NA'))) As City,
(COUNT(1)-COUNT(nullif(Country,'NA'))) As Country,
(COUNT(1)-COUNT(nullif(Medal,'NA'))) As Medal,
(COUNT(1)-COUNT(nullif(notes,'NA'))) As notes
FROM final_joined_distinct

-- COMMAND ----------

-- Refugee and Unknown NOCs have 'NA' regions 
SELECT *
FROM final_joined_distinct 
WHERE Region = 'NA'

-- COMMAND ----------

-- Change literal 'NA' strings in Medal column to null
UPDATE final_joined
SET Medal = NULL WHERE Medal = 'NA'

-- COMMAND ----------

-- Change NA in Medal column to null
UPDATE final_joined_distinct
SET Medal = NULL WHERE Medal = 'NA'

-- COMMAND ----------

-- Change NA in Region column to null
UPDATE final_joined
SET Region = NULL WHERE Region = 'NA'

-- COMMAND ----------

-- Change NA in Region column to null
UPDATE final_joined_distinct
SET Region = NULL WHERE Region = 'NA'

-- COMMAND ----------

-- count number of empty values in Medal column 
SELECT 
  COUNT(*)-COUNT(Medal) As Medal
FROM final_joined

-- COMMAND ----------

SELECT 
  COUNT(*)-COUNT(Medal) As Medal
FROM final_joined_distinct

-- COMMAND ----------

-- Display non-duplicate final table 
SELECT *
FROM final_joined_distinct 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Distinct items 

-- COMMAND ----------

--Count distinct items in each column 
SELECT 
COUNT(DISTINCT ID) AS IDs, 
COUNT(DISTINCT Name) AS Names,
COUNT(DISTINCT Sex) AS Sex, 
COUNT(DISTINCT Team) AS Teams, 
COUNT(DISTINCT NOC) AS NOCs, 
COUNT(DISTINCT Region) AS Regions, 
COUNT(DISTINCT Sport) AS Sports, 
COUNT(DISTINCT Event) AS Events, 
COUNT(DISTINCT Games) AS Games, 
COUNT(DISTINCT City) AS Cities, 
COUNT(DISTINCT Country) AS Countries, 
COUNT(DISTINCT Year) AS Years, 
COUNT(DISTINCT Season) AS Season,
COUNT(DISTINCT Medal) AS Medals
FROM final_joined 

-- COMMAND ----------

--Count distinct items in each column (same as above)
SELECT 
COUNT(DISTINCT ID) AS IDs, 
COUNT(DISTINCT Name) AS Names,
COUNT(DISTINCT Sex) AS Sex, 
COUNT(DISTINCT Team) AS Teams, 
COUNT(DISTINCT NOC) AS NOCs, 
COUNT(DISTINCT Region) AS Regions, 
COUNT(DISTINCT Sport) AS Sports, 
COUNT(DISTINCT Event) AS Events, 
COUNT(DISTINCT Games) AS Games, 
COUNT(DISTINCT City) AS Cities, 
COUNT(DISTINCT Country) AS Countries, 
COUNT(DISTINCT Year) AS Years, 
COUNT(DISTINCT Season) AS Season,
COUNT(DISTINCT Medal) AS Medals
FROM final_joined_distinct 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Shared names

-- COMMAND ----------

-- Determine if some players have same name 
SELECT Name, COUNT(DISTINCT ID) AS count 
FROM final_joined_distinct
GROUP BY Name
ORDER BY count DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Min and Max 

-- COMMAND ----------

-- Minimum and maximum year, age, height, weight 
SELECT MIN(Year), MAX(Year),  MIN(Age), MAX(Age), MIN(Height), MAX(Height), MIN(Weight), MAX(Weight)
FROM final_joined_distinct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Count

-- COMMAND ----------

-- Total medals per country
SELECT Team, COUNT(Medal) AS medal_count 
FROM final_joined_distinct 
GROUP BY Team
ORDER BY medal_count DESC

-- COMMAND ----------

-- Total medals per team 
SELECT Team, COUNT(Medal) AS medal_count 
FROM final_joined_distinct 
GROUP BY Team
ORDER BY medal_count DESC
LIMIT 10

-- COMMAND ----------

-- Count of each type of medal in top 10 teams 
SELECT Team, Medal, COUNT(Medal) AS medal_count 
FROM final_joined_distinct 
WHERE Team IN ('United States', 'Soviet Union', 'Germany','Great Britain', 'France', 'Italy', 'Sweden', 'Australia', 'Canada', 'Hungary')
GROUP BY Team, Medal
ORDER BY medal_count DESC

-- COMMAND ----------

--Number of medals per year
SELECT Year, Medal, COUNT(Medal) AS medal_count
FROM final_joined_distinct
WHERE Medal IS NOT NULL
GROUP BY Year, Medal
ORDER BY Year

-- COMMAND ----------

-- Which sports do not have mixed genders?  
SELECT 
  Sport, 
  COUNT(CASE WHEN Sex = 'M' THEN 1 END) AS male_count, 
  COUNT(CASE WHEN Sex = 'F' THEN 1 END) AS female_count 
  --(COUNT(CASE WHEN Sex = 'F' THEN 1 END)) / (COUNT(CASE WHEN Sex = 'M' THEN 1 END)) AS female_male_ratio, 
  --(COUNT(CASE WHEN Sex = 'M' THEN 1 END)) / (COUNT(CASE WHEN Sex = 'F' THEN 1 END)) AS male_female_ratio 
FROM(  SELECT DISTINCT ID, Name, Sex, Sport 
       FROM final_joined_distinct)
GROUP BY Sport
ORDER BY male_count DESC

-- COMMAND ----------

--Player with the most medals
Select Name, Medal, COUNT(Medal) AS medal_count 
FROM final_joined_distinct
WHERE Medal IS NOT NULL
GROUP BY Name, Medal
ORDER BY medal_count DESC

-- COMMAND ----------

-- Number of Teams per Game 
Select Games, COUNT(DISTINCT Team) AS team_count 
FROM final_joined_distinct
GROUP BY Games
ORDER BY team_count DESC

-- COMMAND ----------

-- Number of Events per Game 
Select Games, COUNT(DISTINCT Event) AS event_count 
FROM final_joined_distinct
GROUP BY Games
ORDER BY event_count DESC

-- COMMAND ----------

--Number of athletes participating every year and season
Select Year, Season, COUNT(DISTINCT ID) AS athlete_yearly_count 
FROM final_joined_distinct
GROUP BY Year, Season
ORDER BY Year ASC

-- COMMAND ----------

--Number of sports every year and season
Select Year, Season, COUNT(DISTINCT Sport) AS sports_yearly_count 
FROM final_joined_distinct
GROUP BY Year, Season
ORDER BY Year ASC

-- COMMAND ----------

--Number of men and women athletes every year
Select Year, Sex, COUNT(Sex) AS gender_yearly_count 
FROM final_joined_distinct
GROUP BY Year, Sex
ORDER BY Year ASC

-- COMMAND ----------

-- Total number of male and female athletes 
SELECT Sex, COUNT(DISTINCT ID) AS gender_count 
FROM final_joined_distinct
GROUP BY Sex

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average

-- COMMAND ----------

--Average age, height, weight of players in each region
SELECT NOC, Region, AVG(Age) AS avg_age ,AVG(Height) AS avg_height , AVG(Weight) AS avg_weight
FROM final_joined_distinct
GROUP BY NOC, Region  
--ORDER BY avg_age DESC

-- COMMAND ----------

--Average age male and female players in each region
SELECT Region, Sex, AVG(Age) AS avg_age
FROM final_joined_distinct
GROUP BY Region, Sex  
ORDER BY avg_age DESC

-- COMMAND ----------

--Average weight of male and female players in each region
SELECT Region, Sex,  AVG(Weight) AS avg_weight
FROM final_joined_distinct
GROUP BY Region, Sex  
ORDER BY avg_weight DESC

-- COMMAND ----------

--Average height of male and female players in each region
SELECT Region, Sex, AVG(Height) AS avg_height 
FROM final_joined_distinct
GROUP BY Region, Sex  
ORDER BY avg_height DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Distribution

-- COMMAND ----------

-- Gender distirbution in sports
SELECT Sport, Sex, COUNT(Sex) as gender_count
FROM final_joined_distinct
GROUP BY Sport, Sex
ORDER BY gender_count DESC

-- COMMAND ----------

-- Age distribution in each gender  
SELECT Age, Sex
FROM final_joined_distinct

-- COMMAND ----------

-- Height distribution in each female 
SELECT Height, Sex
FROM final_joined_distinct

-- COMMAND ----------

-- Weight distribution in each female 
SELECT Weight, Sex
FROM final_joined_distinct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Questions

-- COMMAND ----------

-- Do each distinct game only occur in one City/Country? (Yes)
Select City, Games, COUNT(DISTINCT Games) AS game_count 
FROM final_joined_distinct
GROUP BY City, Games 
ORDER BY game_count DESC

-- COMMAND ----------

-- Do each player have at least one game? (Yes)
Select Name, COUNT(Games) AS games_count 
FROM final_joined_distinct
GROUP BY Name
ORDER BY games_count ASC

-- COMMAND ----------

-- What info does the notes column have? 
SELECT *
FROM final_joined_distinct
WHERE notes IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IV. Examine final joined data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = sql("SELECT * FROM final_joined_distinct")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.help("ls")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Number of partitions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are 8 number of partitions (default parallelism in Databricks community edition)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # check how many partitions the final aggregated data has
-- MAGIC 
-- MAGIC df.rdd.getNumPartitions()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("SELECT * FROM final_joined_distinct").rdd.getNumPartitions()

-- COMMAND ----------

SELECT Event, COUNT(*) AS count 
FROM final_joined_distinct
GROUP BY Event

-- COMMAND ----------



-- COMMAND ----------

-- Try repartition 
CREATE OR REPLACE TEMPORARY VIEW final_joined_distinct2
  AS
SELECT /*+ REPARTITION(4) */ * 
FROM final_joined_distinct

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("SELECT * FROM final_joined_distinct2").rdd.getNumPartitions()

-- COMMAND ----------

SELECT Event, COUNT(*) AS count 
FROM final_joined_distinct2
GROUP BY Event

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %timeit sql("SELECT * from final_joined_distinct2").describe()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %timeit sql("SELECT * from final_joined_distinct").describe()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Show partitions 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""desc default.final_joined_distinct""").show(truncate=False)

-- COMMAND ----------


