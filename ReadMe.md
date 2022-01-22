# Building pipeline for Customer Churn analysis

## Reference Architecture of the Demo
<img src="/images/Arch.png" />

<b>
As a data engineering team member at a Telecom company, we have been tasked to build an end to end data pipeline in snowflake to support customer churn analysis by data science team. For this demo we have some customer data that our data science team would need.  We are responsible to build a feature store for the data science team. </b>


# Challenges
* As Data Engineers we want to build an end to end pipeline within snowflake and reduce the overall cost and technology footprints so that there are less point of failures
* On one hand we have customer billing and demographics data saved in semi structured format (PARQUET) and on other we have unstructured data in the form of emails
* We want to simplify the data pipeline by ingesting and processing the data closer within snowflake
But there are multiple developers persona in our team. Some know SQL, Some are Java and python professionals
* We also need to make data scientist’s life easier by cleaning, formatting, transforming and creating a feature store for them to refer in churn analysis

# Solution Overview

We are going to built an end to end data pipeline to built a BI, anaylytics data model and a feature store using new and old snowflake features. There are six features highted in this demo -

1. Snowpipe ingest, with latency improvements
2. Schema detection using INFER_SCHEMA
3. COPY HISTORY dashboard, new feature to visualize data loading with one click
4. Unstructured file data support and its processing using JAVA UDF
5. Snowpark for Transformations and Data engineering
6. Serverless tasks. Update features store as an example

# Setup

There are two pieces of this lab. One that needs to be run on the snowflake UI as worksheets and other that needs to be run on the snowpark jupyter notebook

## SQL setup and steps

1. Follow the pre steps document to setup your own S3 bucket as external stage (telco_data_stream) and connect to a common S3 exernal stage (S3_TELCO_DATA, unstructured_customer_data, unstructured_dependency_jars )
2. Snowflake new UI (Snowsight) allows you to import the SQL file in the worksheet. Just click on the left hand side menu "Worksheets" and than click on the ... (elipses) to select ```Create worksheet from SQL File``` . You should do this step for all the sql file in the ```DE_TKO_HOL``` folder
3. Start the lab by running the worksheets in the order of the series  01, 02, 03 ..
4. You will need a jupyter notebook environment for snowpark. Please follow the instructions if you don't already have one [in medium blog here]
5. Have your HOL Pre Steps documents handy as we will need it in the lab

### Your snowflake DE_TKO_HOL folder should look like this

<img src="/images/worksheets.png" />

[snowpipe setup here]: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3.html
[in medium blog here]: https://medium.com/snowflake/from-zero-to-snowpark-in-5-minutes-72c5f8ec0b55