/*
Created By: Parag Jain

Created For: End to End Data Engineering HOL

Snowpipe can ingest data from any cloud object store using REST API or
event notification channel. We will setup the auto_ingest in this script

Update your Bucket name and AWS credentials before running this script
*/

USE ROLE SYSADMIN;
USE SCHEMA DE_DEMO_TKO_HOL.TELCO;
USE WAREHOUSE LAB_L_WH;


-- Create a external stage to point it to cloud storage
CREATE STAGE IF NOT EXISTS telco_data_stream
url = 's3://<your bucket name>/telco_data_stream'
    credentials = (aws_key_id = '<Your-AWS-KEY-From-Pre-step-Document>' 
                   aws_secret_key = '<Your-AWS-SECRET-From-Pre-step-Document>')
FILE_FORMAT=(TYPE=PARQUET);

ls @telco_data_stream;

-- Create snowpipe for auto ingestion, note that we are using MATCH_BY_COLUMN_NAME 
-- this way the table and file schema can be mapped for data load, snowpipe will
-- keep track of the files to load and those that are already uploaded
CREATE PIPE IF NOT EXISTS 
    TELCO_PIPE 
    AUTO_INGEST=TRUE 
    AS 
    COPY INTO 
        RAW_PARQUET 
    FROM 
        '@telco_data_stream/upload'
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
 

-- SNOWPIPE auto ingest requires you to update the S3 bucket settings with the 
-- snowpipe sqs ARN, so that snowpipe can be notified once new data is available
-- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3.html
-- show pipes;
DESC PIPE TELCO_PIPE;

-- Go to your bucket Properties and scroll down to event notification to add
-- the snowpipe sqs ARN found in the "notification_channel" column
-- prefix > telco_data_stream/upload


SELECT SYSTEM$PIPE_STATUS('TELCO_PIPE');
--alter pipe telco_pipe refresh;

LS @telco_data_stream/upload;

-- We created a raw parquet table inside the public schema of this database
-- in the pre step. We will use this to simulate the streaming data
-- Lets pause the pipe before starting the simulation

alter pipe TELCO_PIPE set pipe_execution_paused = true;

-- Start Streaming simulation ---

copy into @telco_data_stream/upload/ from PUBLIC.RAW_PARQUET_TELCO
    file_format = (type = 'parquet')
    header = true
    include_query_id=true
; 

-- Lets resume the pipe and wait for few seconds to see if the 
-- data is now loaded in the raw parquet table
alter pipe TELCO_PIPE set pipe_execution_paused = false;


--check pipe status
SELECT SYSTEM$PIPE_STATUS('TELCO_PIPE');


-- Snowpipe keeps track of the processed files here
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'RAW_PARQUET',
  START_TIME=>DATEADD(HOUR, -1, CURRENT_TIMESTAMP))) ORDER BY LAST_LOAD_TIME DESC;


-- check if the data is uploaded into raw_parquet table
SELECT * FROM RAW_PARQUET;

