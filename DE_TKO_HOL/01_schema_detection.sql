/*
Created By: Parag Jain

Created For: End to End Data Engineering HOL

SCHEMA DETECTION is the new feature in snowflake. 
Use INFER_SCHEMA table function TO CREATE TABLE without writing 
column and data types. This features works great for raw ingestion

*/

USE ROLE SYSADMIN;
USE SCHEMA DE_DEMO_TKO_HOL.TELCO;
USE WAREHOUSE LAB_L_WH;

CREATE FILE FORMAT IF NOT EXISTS MY_PARQUET_FORMAT
  TYPE = PARQUET;

CREATE
    TABLE IF NOT EXISTS RAW_PARQUET USING TEMPLATE (
        SELECT
            ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM
            TABLE(
                INFER_SCHEMA(
                    LOCATION => '@S3_TELCO_DATA/sample_data/schema_detection.parquet',
                    FILE_FORMAT => 'MY_PARQUET_FORMAT'
                )
            )
    );

-- just in case you missed the pre steps for demo setup
