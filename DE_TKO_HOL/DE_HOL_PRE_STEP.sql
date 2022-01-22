/* 

Created By: Parag Jain
Created For: End to End data engineering in snowflake - HOL

*/

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS DE_DEMO_TKO_HOL;
CREATE SCHEMA IF NOT EXISTS TELCO;

USE DATABASE DE_DEMO_TKO_HOL;
USE SCHEMA DE_DEMO_TKO_HOL.TELCO;


CREATE WAREHOUSE IF NOT EXISTS LAB_L_WH 
    WITH WAREHOUSE_SIZE = 'LARGE' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'STANDARD' 
    COMMENT = 'WAREHOUSE CREATED FOR DEMO';

USE WAREHOUSE LAB_L_WH;

CREATE TABLE IF NOT EXISTS PUBLIC.RAW_PARQUET_TELCO (
	CUSTOMERID VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	CITY VARCHAR(16777216),
	"PHONE SERVICE" VARCHAR(16777216),
	"MULTIPLE LINES" VARCHAR(16777216),
	LATITUDE VARCHAR(16777216),
	"ONLINE SECURITY" VARCHAR(16777216),
	"SENIOR CITIZEN" VARCHAR(16777216),
	"MONTHLY CHARGES" VARCHAR(16777216),
	"STREAMING MOVIES" VARCHAR(16777216),
	"PAYMENT METHOD" VARCHAR(16777216),
	"LAT LONG" VARCHAR(16777216),
	"TENURE MONTHS" VARCHAR(16777216),
	COUNT VARCHAR(16777216),
	"PAPERLESS BILLING" VARCHAR(16777216),
	"CHURN VALUE" VARCHAR(16777216),
	"TECH SUPPORT" VARCHAR(16777216),
	"CHURN LABEL" VARCHAR(16777216),
	PARTNER VARCHAR(16777216),
	DEPENDENTS VARCHAR(16777216),
	"INTERNET SERVICE" VARCHAR(16777216),
	"STREAMING TV" VARCHAR(16777216),
	CONTRACT VARCHAR(16777216),
	"CHURN SCORE" VARCHAR(16777216),
	LONGITUDE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	"ONLINE BACKUP" VARCHAR(16777216),
	"DEVICE PROTECTION" VARCHAR(16777216),
	"TOTAL CHARGES" VARCHAR(16777216),
	CLTV VARCHAR(16777216),
	"CHURN REASON" VARCHAR(16777216),
	STATE VARCHAR(16777216),
	"ZIP CODE" VARCHAR(16777216)
);    

CREATE FILE FORMAT IF NOT EXISTS PUBLIC.TELCO_CSV_FORMAT 
    TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');


-- CREATE STAGE FOR DEMO FILES
-- For Snowflake Colleague's only, this is a restricted user and key. 
-- Its rotated frequently. Secret and Key will be renewed after this lab.

CREATE STAGE IF NOT EXISTS S3_TELCO_DATA
url = 's3://sfpjain-us-west-2/shared/telco/'
    credentials = (aws_key_id = 'COMMON-AWS-KEY-From-Pre steps-Document' 
                   aws_secret_key = 'COMMON-AWS-KEY-From-Pre steps-Document')
;                   


-- Create Stage to load unstructured data (RFC822 format email)
CREATE STAGE IF NOT EXISTS unstructured_customer_data
    url = 's3://sfpjain-us-west-2/shared/telco/email'
    credentials = (aws_key_id = 'COMMON-AWS-KEY-From-Pre steps-Document' 
                   aws_secret_key = 'COMMON-AWS-KEY-From-Pre steps-Document')
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE)
;

-- Stage to store the jar files for user defined function
CREATE STAGE IF NOT EXISTS unstructured_dependency_jars
url = 's3://sfpjain-us-west-2/shared/telco/jars'
    credentials = (aws_key_id = 'COMMON-AWS-KEY-From-Pre steps-Document' 
                   aws_secret_key = 'COMMON-AWS-KEY-From-Pre steps-Document')
;


-- You should see some rows (7 or more) after running this list command for the stage
LS @S3_TELCO_DATA;

TRUNCATE TABLE PUBLIC.RAW_PARQUET_TELCO;

-- Load 100K rows from the raw data in the stage

COPY INTO PUBLIC.RAW_PARQUET_TELCO from @S3_TELCO_DATA/sample_data/telco_data.csv   FILE_FORMAT  = (FORMAT_NAME = 'PUBLIC.TELCO_CSV_FORMAT') ;  

-- we will use this table to stream synthetic data for snowpipe consumption




CREATE OR REPLACE PROCEDURE "ONEHOTENCSQL"("DBNAME" VARCHAR(16777216), "SCHNAME" VARCHAR(16777216), "TBLNAME" VARCHAR(16777216), "COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;
  var msg = "Info: OneHot encoding for column " + COLNAME + " completed";
  
  // list distinct column values
  var listSql = "select distinct " + COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  var counter = 0 ;
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      var colName = COLNAME + "_" + col.replace(/\\((.*?)\\)/g,'''').replace(/ /g,'''');

      //add encoded col
      var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
      try 
      {
          var addCol = snowflake.execute({ sqlText: addColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \\n Info: Column ''" + colName + "'' already exists ";
 
      }

      //update encoded col
      var updColSql = "update " + tblName + " set " + colName + "= (case lower(" + COLNAME + ") when lower(''" + col +"'') then 1 else 0 end);";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \\n Info: columns updated: " + counter + " /\\n Warning: " + err ;
      }
  }

    return msg;

';

CREATE OR REPLACE PROCEDURE "ORDINALENCSQL"("DBNAME" VARCHAR(16777216), "SCHNAME" VARCHAR(16777216), "TBLNAME" VARCHAR(16777216), "COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
  var counter = 0 ;
  var msg = "Ordinal Encoding completed Successfully ";
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;

  // Create new ordinal encoding column
  var colName = COLNAME + "_ordinal";

  //add encoded col
  var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
  try 
  {
      var addCol = snowflake.execute({ sqlText: addColSql });
  }
  catch(err)
  {
      counter = 0;
      msg += " , Info: Column ''" + colName + "'' already exists ";
  }

  // list distinct column values
  var listSql = "select distinct "+ COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      counter += 1;
      
      //update ordinal encoded col
      var updColSql = "update " + tblName + " set " + colName + " = " + counter + " where " + COLNAME + " = ''" + col +"'' ;";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
      }
      catch(err)
      {
          msg = "Warning: " + updColSql + " : " + err;
      }
  }

    return msg;

';