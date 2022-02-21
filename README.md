# Marketplaats Data Engineer Test

## Summary

The task is to load data from multiple sources and find data analystics  after cleaning and data trasnformation with optimized parrallel data processing using spark framework. 
Code is written the solution in pyspark script. There is no any other dependecies required apart from Spark library and developed code package.

 
## Steps to execute task:

Asssuming spark 2.7 is already installed.  
Go to project root folder and exeute below command:

spark-submit --master local --py-files package.zip jobs/marketplaats.py --credit_fpath ./data/credit_limit_events.csv --country_fpath ./data/countries.csv --usersituation_fpath ./data/user_situation.csv --output_fpath result_output

#py-file: 
dependencies are zipped in util.zip file 
#credit_fpath:
csv file path to credit_limit_events.csv
#country_fpath:
csv file path to countries.csv
#usersituation_fpath:
csv file path to user_situation.csv
#output_fpath:
result output folder for this task

#pyspark main file:
jobs/marketplaats.py
#util packege:
written cleaning and tranformation function
#depencedencie:
spark session initialisation
#pytranse:
reading and writing data code
#package.zip:
ziiped all packeges from project
#result_output:
result output folder where spark job will write final data
#data: 
source data credit_limit_events.csv, countries.csv and user_situation.csv

#Project Structure:

root/
 |-- dependencies/
 |   |-- spark.py
 |-- jobs/
 |   |-- marketplaats.py
 |-- pytranse/
 |   |-- read.py
 |   |-- write.py
 |-- utils
 |   |--util.py
 |   package.zip
 |   result_output
 |   data
 |   README.md

* What are the key phases in the data pipeline?
following is the key phases:
1. data source loading
2. data cleaming
3. data tranformation
4. data analystic
5. final destination
* What do you do to manage data quality issues?
#1. Duplicate data:
Duplicate data is when the same data is entered multiple times, but in slightly different ways. Duplicate data is often created when extracting data from multiple siloed systems and merged together in a data warehouse, creating ‘copies’ of the same record. Duplication may produce skewed or incorrect insights when they go undetected.
#2. Inconsistent formats:
Storing the same type of data in inconsistent formats is a common quality issue.
#3. Incomplete information:
This data quality issue occurs when crucial pieces of information are missing, either as a result of failure to input it at the source system, or as a result of ETL processes.
#4. Data inconsistency:
Data inconsistency is the result of storing data in the same field that is either in a different language or in different units.
#5. Inaccurate data:
This is one of the most difficult data quality issues to spot, and occurs when the format is correct and every value is complete, but potential mis-spellings exist or the data is simply inaccurat
#6. Invalid data
Data Invalidity is when your data can’t possibly correct based on simple rules or logic.
#7. Data imprecision:
Data imprecision, or lack of precision is when data has been stored at a summarized level, as a result of an ETL process, that does not enable users to get to the level of detail they need for analysis.

* What can you do to track data lineage in your approach?

## Data lineage across the pipeline
To capture end-to-end lineage, data has to be tracked across every stage and process in the data pipeline. Here are the stages across which data lineage is performed:

#Data Gathering Stage
The data gathering or ingestion stage is where data enters the core system. Data lineage can be used to track the vitals of the source and destination systems to validate the accuracy of the data, mappings, and transformations. Tracking the systems closely also helps in the easier identification of bugs.

#Data Processing Stage
Data processing takes up a huge percentage of the entire process of creating data solutions. It involves multiple transformations, filters, data types, tables, and storage locations. Recording metadata from each step doesn’t just help in compliance and production speed but also makes the development process richer and more productive. It enables developers to analyze the causes behind the success or failures of processes in higher detail.

#Data Storing and Access Stage
Organizations usually deploy large data lakes to store their data. Data lineage can be used to track the access permissions, vitals of endpoints, and data transactions. This will increase the degree of automation of security and compliance, which is a huge bonus given the size and complexity of data lakes.

#Data Querying Stage
Users raise multiple data queries with a range of functions like joins and filters. Some functions can be heavy on the processors and therefore, less efficient. Data lineage can observe the queries to track and validate the processes and different versions of data resulting from them. Meanwhile, it also helps in optimizing the queries and provides reports including instances of optimal solutions. 

* How will you ensure and validate scalability once you release this pipeline to production?
Automate continuous delivery through a delivery using CI/CD pipeline

* How will you ensure throughput performance?
1. Initial Capture should be Robust and Extract Data Using Bulk, Parallel Processing:
2. Identify Automatic, Reliable Ways to ensure proper Synchronization - Avoid Manual Sync:
3. Decouple Initial Capture and CDC process dependency to Improve Performance

* How easy is it for other Data Engineers to understand and work on top your code?
1. use documentaion as mutch as possible
2. create method for each task
3. avoid hard coding
4. keep descriptive variable and method name