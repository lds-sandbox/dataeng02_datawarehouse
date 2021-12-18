# Data Warehouse ETL using Python and AWS Redshift

## Introduction

This project demonstrates an ETL process that reads attribute information and log data using Python and writes the information into an AWS Redshift data warehouse.  The data represents songs in a streaming service called Sparkify and log data that contains information about who listened to which songs and other related attributes.

The warehouse and related infrastructure is built using the boto3 package, representing an IAC process.

This project was developed as part of the Udacity Data Engineering nanodegree program.

## How to install
There is no installation package.  The folder structure and all of the files can be downloaded from the repository and saved directly on a local computer.  The code expects the files to be saved in the /home/workspace folder of the local machine

## How to use
* The database is (re)created and refreshed by executing the aws_ian_initialize.py and create_tables.py scripts from the command line.
* The database is removed from your AWS account by executing the aws_iac_cleanup.py script.
* All scripts expect an associated configuration file to house the IAM users keys and parameters for the database.
* The <span>etl.py</span> script is the main script that reads and process all of the files in the data folder

## Technologies used
package (version) <br>
python (3.6.3) <br>
boto3 (1.9.7) <br>
json (2.0.9) <br>
psycopg2 (2.7.4) <br>
pandas (0.23.3) <br>
