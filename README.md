# Sparkify Database

Sparkify is a music streaming startup that has seen a great growth in it's userbase. They've decided to utilize the powers of the cloud as to help them with their expanding business. For now, all their data is stored on AWS S3. While this is good, for performing some analytical tasks such as, finding out what songs are popular right now among it's users, what artists are being listened to the most, etc., we can make use of other AWS Services. One such service is AWS Redshift.

## Objective of the project

The objective of this project is to build an ETL (Extract, Transform, Load) pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transforms the data into a set of dimensional tables for the analytics team to continue finding insights on the data.

## Explanation of the files

- _create_tables.py_ : This file contains the python script to create the tables in our Redshift cluster. As a precautionary measure, we have a _drop_tables_ function that is used to drop the tables in case they are already present. This is useful in case we are creating the tables again and re-entering the data. If we were not to drop the tables beforehand, it could lead to the data being entered into the same table twice.
- _etl.py_ : This file contains the python script for creating the ETL pipeline. It is using this file that we COPY data into the Stating Tables and also INSERT data into the fact and dimension tables that we created in _create_tables.py_
- _sql_queries.py_ : This file contains the bulk of our code. As the name suggests, this file has all the code regarding the various steps such as Dropping tables, Creating Tables, Copying into Staging tables, and finally Inserting into tables.
- _dwh.cfg_ : This file contains information that is used while creating the Redshift cluster itself. It contains important details like the _HOST_, _DB_NAME_, _DB_PASSWORD_ etc., which are used to connect to our Redshift Cluster and the most important detail in this file the _IAM ROLE'S ARN_ which should not be shared with anyone.

## Sequence for running the project

1. Firstly we must create an IAM Role which has access to Redshift cluster and S3. We then have to create a Redshift cluster. Now add the details of these Redshift cluster and the IAM Role into our _dwh.cfg_ file.
2. Next we run the _create_tables.py_ to create the tables in our cluster.
3. Once we're done creating the tables successfully, we will then run the script _etl.py_ to build our ETL pipeline.
4. With this we have built our ETL pipeline and have successfully inserted data into our tables. These tables will now be available for querying from the AWS Redshift interface.
5. We connect to the database using the credentails we mentioned in _dwh.cfg_ and run our queries as needed!
