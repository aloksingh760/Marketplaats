# Data Engineer Test

## Purpose

In Marktplaats we want to build a diverse and skilled team to improve, maintain and support our big data analytics platform. With more than 11 million unique monthly active users, we want to continuously improve our platform to fulfil the analytical needs of all the departments in the company.

As a Data Engineer, you will enable and support the Data Science and Data Analytics teams by designing and implementing scalable and resilient data products. With this assignment, we would like to invite you to show us your skills and express your thinking on designing and building data pipelines.

## Context

In this repo, you'll find the following files:

- `credit_limit_events.csv`: Contains the information of the userscredit limit updates from our event store;
- `countries.csv`: a lookup table for countries;
- `user_situation.csv`: a lookup table that describes specific financial situations that may be applicable to a user.

These files are input for denormalised big data store that summarises the latest credit limit and associated data for each user. This big data store can be a data warehouse or a query engine for a data lake.

The result dataset will be used by Data Scientists and Product Analysts who know how to write PySpark and SQL. These users appreciate datasets/file formats that a) contain a schema b) have good data quality.


## Assignment

We would like to ask you to create a job/data pipeline that cleans and transforms the given files into an aggregated view of the latest credit limit and associated data for each user in a single denormalised dataset/file.

You are free to implement your pipeline using any transformation framework.

Please consider the following questions when building your approach:

* What are the key phases in the data pipeline?
* What do you do to manage data quality issues?
* What can you do to track data lineage in your approach?
* How will you ensure and validate scalability once you release this pipeline to production?
* How will you ensure throughput performance?
* How easy is it for other Data Engineers to understand and work on top your code?


## Constraints

* Please complete the test within 5 business days. **Please don't spend more than 4 hours on this test. We are not looking for an extensive deliverable. We want to understand your way of thinking and see a sample of your code**.
* Use either Scala or Python. 
* Use any libs / tools you like. Using Spark is generally recommended, but if you feel that something else is better suited to the job, go for it.
* We need to be able to run your code. Please include sufficient details in the README for us to build and run/execute the data pipeline.
* This repository is created especially for you, so you can use it in any way that you believe serves this test best. Please update this README to provide instructions, notes and/or comments for us.

## Deliverable

Please submit a pull request with:

* The files that are necessary to run your data pipeline
* The output files that result after running your pipeline.
* The `requirements.txt` file with any package dependencies that are necessary to deploy/run your solution (if necessary)
* Any other files that are necessary to deploy & build your pipeline
* An added section to the `README` that describes how to build & run your solution
* An added section to the `README` that makes any important notes you consider relevant for us to understand your solution
* A section that describes how the result files or dataset could be used in a big data store/ data warehouse/ query engine to support the needs of the Data Scientists and /or Product Analysts.
* A section that describes the elements that you purposefully left out of the scope of the test and the reasoning behind these decisions.

## Questions?

If you have any questions, please send an email to <dieserrano@eclassifiedsgroup.com>.

## Finished?

1. Please submit a pull request once you are done
2. Send an email to dieserrano@eclassifiedsgroup.com to let us know you're done.

Good Luck!

Copyright (C) 2001 - 2021 by Marktplaats BV an Adevinta company. All rights reserved.
