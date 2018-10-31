// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #### Hooli XYZ Hackathon Presentation: A solution for predicting and preventing compressor failures.
// MAGIC 
// MAGIC ######  Presented on October 31, 2018 (Happy Hooli-ween)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC What we are going to do first is take the Compression data file and import the data into a Databricks table. 
// MAGIC 
// MAGIC The benefit of this is that we can do some additional querying and analysis (cough cough machine learning cough) against the table, since the file itself is hard to consume as-is. (Context: Excel can handle a little over ~1M rows and this file had 16M rows)

// COMMAND ----------

// Declare the values for your Azure SQL database

val jdbcUsername = "hacker0401@hackerteam04"
val jdbcPassword = "WlB-Jk1b"
val jdbcHostname = "hackerteam04.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase ="compdata"

// COMMAND ----------

// Import Java things
import java.util.Properties

// Declare connection properties
val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

// Declare storage information and file location

val storage_account_name = "stgteam04"
val storage_account_access_key = "+GbbJnJCmQJFrMP8ijkvwhLRM6l0ETxDKufxuB84V0OiqLzWp6IxOPGRYwtWrA+yuVsOyxKTnbKKDAh3iIBDiA=="
val file_location = "wasbs://stgteam04.blob.core.windows.net/compressordata/compressors_eagleford_travis_20181025142853.txt"
val file_type = "csv"

// COMMAND ----------

// Using spark configuration

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

// Option 1: Insert the compressor data file into to Databricks

// Declaring the file path and saving the csv as a dataframe
val fpath = "/mnt/compressordata/compressors_eagleford_travis_20181025142853.txt"
val df = spark.read.format("csv").option("header", "true").option("delimiter","\t").option("inferSchema", "true").load(fpath)

// Pushing the datframe to a temporary table 
df.createOrReplaceTempView("compressordata");

// Created a table from the temporary table. We can push this table to our SQL database.
// Looks long and ugly because Databricks doesn't like spaces.
// Limited to 100000 rows for sake of time and my personal sanity.  
spark.sql("drop table if exists tblcompressordata10K");
spark.sql("create table tblcompressordata10K as select Id, `Asset Name` as asset_name, `Local Timestamp` as local_timestamp, `UTC Milliseconds` as UTC_milliseconds, `Compressor Oil Pressure` as compressor_oil_pressure, `Compressor Oil Temp` as compressor_oil_temp, `Compressor Stages` as compressor_stage, `Cylinder 1 Discharge Temp` as cylinder_1_discharge_temp, `Cylinder 2 Discharge Temp` as cylinder_2_discharge_temp, `Cylinder 3 Discharge Temp` as cylinder_3_discharge_temp, `Cylinder 4 Discharge Temp` as cylinder_4_discharge_temp, `Downtime Hrs Yest` as downtime_hrs_yest, `Engine Oil Pressure` as engine_oil_pressure, `Engine Oil Temp` as engine_oil_temp, `Facility Desc` as facility_desc, `Facility ID` as facility_id, `Fuel Pressure` as fuel_pressure, `Gas Flow Rate` as gas_flow_rate, `Gas Flow Rate_RAW` as gas_flow_rate_raw, Horsepower, `Last Successful Comm Time` as last_successful_comm_time, `Max Discharge Pressure` as max_discharge_pressure, `Max Gas Flowrate` as max_gas_flowrate, `Max RPMs` as max_rpms, `Max Suction Pressure` as max_suction_pressure, `Pct Successful Msgs Today` as pct_successful_msgs_today, RPM, `Run Status` as run_status, `Runtime Hrs` as runtime_hrs, `SD Status Code` as sd_status_code, `Stage 1 Discharge Pressure` as stage_1_discharge_pressure, `Stage 2 Discharge Pressure` as stage_2_discharge_pressure, `Stage 3 Discharge Pressure` as stage_3_discharge_pressure, `Successful Msgs Today` as successful_msgs_today, `Suction Pressure` as suction_pressure, `Suction Temp` as suction_temp, `TOW Comp Name` as tow_comp_name, `Unit Size` as unit_size from compressordata limit 100000");

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC This part is cool because we are taking the Databricks table we just created and pushing that out to our SQL database.
// MAGIC 
// MAGIC Our app consumes data from the SQL database, so it is beneficial to house the data in a central location. 

// COMMAND ----------

// Code that Google told me to use before writing back to SQL
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{SQLContext, SaveMode}
import java.sql.{Connection,DriverManager,ResultSet}

// Update the SQL table with the Databricks table we just created
spark.table("tblcompressordata10K").write.mode(SaveMode.Append).option("truncate", "false").jdbc(jdbc_url, "CompressorData10K", connectionProperties)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Who's ready for some MACHINE LEARNING?
// MAGIC 
// MAGIC Here we are querying data from our SQL database, and then storing those results back in the database to consume in our app and Power BI report.

// COMMAND ----------

// code that google told me to use before writing back to sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{SQLContext, SaveMode}
import java.sql.{Connection,DriverManager,ResultSet}

// creating a temp table with data from a table in the SQL database
val sqlTableDF = spark.read.jdbc(jdbc_url, "realtime_ma", connectionProperties)
sqlTableDF.createOrReplaceTempView("eventsummary")

// these queries create a table under the "data" tab on the left. we can import this resultset back into our sql database
spark.sql("drop table if exists tbleventsummary");
spark.sql("create table tbleventsummary as select 1 as priorityid, facility_id as facilityid, '' as assignedto, NOW() as assigneddate, local_timestamp as createdate, rpm_error_count, stg1_error_count, stg2_error_count, stg3_error_count from (SELECT `Facility ID` as facility_id,`Local Timestamp` as local_timestamp, SUM(case when MA1_stg1 > 0 then case when (ABS(MA1_stg1 - `Stage 1 Discharge Pressure`))/ MA1_stg1 > .1 then 1 else 0 end else 0 end) OVER (ORDER BY `asset name`, `Local Timestamp` ASC ROWS 48 PRECEDING) AS stg1_error_count,SUM(case when MA1_stg2 > 0 then case when (ABS(MA1_stg2 - `Stage 2 Discharge Pressure`))/ MA1_stg2 > .1 then 1 else 0 end else 0 end) OVER (ORDER BY `asset name`, `Local Timestamp` ASC ROWS 48 PRECEDING) AS stg2_error_count,SUM(case when MA1_stg3 > 0 then case when (ABS(MA1_stg3 - `Stage 3 Discharge Pressure`))/ MA1_stg3 > .1 then 1 else 0 end else 0 end) OVER (ORDER BY `asset name`, `Local Timestamp` ASC ROWS 48 PRECEDING) AS stg3_error_count,SUM(case when MA1_rpm > 0 then case when (ABS(MA1_rpm - RPM))/ MA1_rpm > .1 then 1 else 0 end else 0 end) OVER (ORDER BY `asset name`, `Local Timestamp` ASC ROWS 48 PRECEDING) AS rpm_error_count FROM eventsummary) a where stg1_error_count + stg2_error_count + stg3_error_count = 40 order by facility_id, local_timestamp");

// COMMAND ----------

// Code that Google told me to use before writing back to SQL
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{SQLContext, SaveMode}
import java.sql.{Connection,DriverManager,ResultSet}

// Save the result set back to our SQL database under "EventSummaryDetails" table. 
spark.table("tbleventsummary").write.mode(SaveMode.Append).option("truncate", "false").jdbc(jdbc_url, "Event", connectionProperties)

// COMMAND ----------


