# Project 4: Data Pipelines with Airflow

&nbsp;

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. Their data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


Therefore, they'd like us to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They also to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

We'll be able to test our database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results. 

&nbsp;
