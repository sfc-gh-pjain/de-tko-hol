/*
Created By: Parag Jain
Created For: Customer Churn analysis Data Pipeline

This script should be run after the demo is completed.
It truncates the required table and set's up the 

*/


USE ROLE SYSADMIN;
use schema DE_DEMO_TKO_HOL.TELCO;
USE WAREHOUSE LAB_L_WH;

-- For reset the data but not the whole demo objects

TRUNCATE TABLE SERVICES;
TRUNCATE TABLE LOCATION;
TRUNCATE TABLE DEMOGRAPHICS;
TRUNCATE TABLE STATUS;
TRUNCATE TABLE RAW_PARQUET;
DROP TABLE FEATURE_STORE;

alter pipe TELCO_PIPE set pipe_execution_paused = true;

/* This will remove all objects and you have to follow all the presteps */

-- For Full reset only 
-- NOTE ** you will have to register the snowpipe SQS again with the 
-- S3 Bucket event notifications properties

-- DROP DATABASE DE_DEMO_TKO_HOL;