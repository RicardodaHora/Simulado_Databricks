import { Question } from './types';

export const RAW_DATA: Question[] = [
    {
      "question": "A data organization leader is upset about the data analysis teams reports being different from the data engineering teams reports. The leader believes the siloed nature of their organizations data engineering and data analysis architectures is to blame. Which of the following describes how a data lakehouse could alleviate this issue?",
      "alternativas": [
        "A. Both teams would autoscale their work as data size evolves",
        "B. Both teams would use the same source of truth for their work",
        "C. Both teams would reorganize to report to the same department",
        "D. Both teams would be able to collaborate on projects in real-time",
        "E. Both teams would respond more quickly to ad-hoc requests"
      ],
      "answer": "B"
    },
    {
      "question": "Which of the following describes a scenario in which a data team will want to utilize cluster pools?",
      "alternativas": [
        "A. An automated report needs to be refreshed as quickly as possible.",
        "B. An automated report needs to be made reproducible.",
        "C. An automated report needs to be tested to identify errors.",
        "D. An automated report needs to be version-controlled across multiple collaborators.",
        "E. An automated report needs to be runnable by all stakeholders."
      ],
      "answer": "A"
    },
    {
      "question": "Which of the following is hosted completely in the control plane of the classic Databricks architecture?",
      "alternativas": [
        "A. Worker node",
        "B. JDBC data source",
        "C. Databricks web application",
        "D. Databricks Filesystem",
        "E. Driver node"
      ],
      "answer": "C"
    },
    {
      "question": "Which of the following benefits of using the Databricks Lakehouse Platform is provided by Delta Lake?",
      "alternativas": [
        "A. The ability to manipulate the same data using a variety of languages",
        "B. The ability to collaborate in real time on a single notebook",
        "C. The ability to set up alerts for query failures",
        "D. The ability to support batch and streaming workloads",
        "E. The ability to distribute complex data operations"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following describes the storage organization of a Delta table?",
      "alternativas": [
        "A. Delta tables are stored in a single file that contains data, history, metadata, and other attributes.",
        "B. Delta tables store their data in a single file and all metadata in a collection of files in a separate location.",
        "C. Delta tables are stored in a collection of files that contain data, history, metadata, and other attributes.",
        "D. Delta tables are stored in a collection of files that contain only the data stored within the table.",
        "E. Delta tables are stored in a single file that contains only the data stored within the table."
      ],
      "answer": "C"
    },
    {
      "question": "Which of the following code blocks will remove the rows where the value in column age is greater than 25 from the existing Delta table my_table and save the updated table?",
      "alternativas": [
        "A. SELECT * FROM my_table WHERE age > 25;",
        "B. UPDATE my_table WHERE age > 25;",
        "C. DELETE FROM my_table WHERE age > 25;",
        "D. UPDATE my_table WHERE age <= 25;",
        "E. DELETE FROM my_table WHERE age <= 25;"
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer has realized that they made a mistake when making a daily update to a table. They need to use Delta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the older version, they are unable to restore the data because the data files have been deleted. Which of the following explains why the data files are no longer present?",
      "alternativas": [
        "A. The VACUUM command was run on the table",
        "B. The TIME TRAVEL command was run on the table",
        "C. The DELETE HISTORY command was run on the table",
        "D. The OPTIMIZE command was run on the table",
        "E. The HISTORY command was run on the table"
      ],
      "answer": "A"
    },
    {
      "question": "Which of the following Git operations must be performed outside of Databricks Repos?",
      "alternativas": [
        "A. Commit",
        "B. Pull",
        "C. Push",
        "D. Clone",
        "E. Merge"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following data lakehouse features results in improved data quality over a traditional data lake?",
      "alternativas": [
        "A. A data lakehouse provides storage solutions for structured and unstructured data.",
        "B. A data lakehouse supports ACID-compliant transactions.",
        "C. A data lakehouse allows the use of SQL queries to examine data.",
        "D. A data lakehouse stores data in open formats.",
        "E. A data lakehouse enables machine learning and artificial intelligence workloads."
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer needs to determine whether to use the built-in Databricks notebooks versioning or version their project using Databricks Repos. Which of the following is an advantage of using Databricks Repos over the Databricks notebooks versioning?",
      "alternativas": [
        "A. Databricks Repos automatically saves development progress",
        "B. Databricks Repos supports the use of multiple branches",
        "C. Databricks Repos allows users to revert to previous versions of a notebook",
        "D. Databricks Repos provides the ability to comment on specific changes",
        "E. Databricks Repos is wholly housed within the Databricks Lakehouse Platform"
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer has left the organization. The data team needs to transfer ownership of the data engineer's Delta tables to a new data engineer. The new data engineer is the lead engineer on the data team. Assuming the original data engineer no longer has access, which of the following individuals must be the one to transfer ownership of the Delta tables in Data Explorer?",
      "alternativas": [
        "A. Databricks account representative",
        "B. This transfer is not possible",
        "C. Workspace administrator",
        "D. New lead data engineer",
        "E. Original data engineer"
      ],
      "answer": "C"
    },
    {
      "question": "A data analyst has created a Delta table sales that is used by the entire data analysis team. They want help from the data engineering team to implement a series of tests to ensure the data is clean. However, the data engineering team uses Python for its tests rather than SQL. Which of the following commands could the data engineering team use to access sales in PySpark?",
      "alternativas": [
        "A. SELECT * FROM sales",
        "B. There is no way to share data between PySpark and SQL.",
        "C. spark.sql(\"sales\")",
        "D. spark.delta.table(\"sales\")",
        "E. spark.table(\"sales\")"
      ],
      "answer": "E"
    },
    {
      "question": "Which of the following commands will return the location of database customer360?",
      "alternativas": [
        "A. DESCRIBE LOCATION customer360;",
        "B. DROP DATABASE customer360;",
        "C. DESCRIBE DATABASE customer360;",
        "D. ALTER DATABASE customer360 SET DBPROPERTIES ('location' = '/user');",
        "E. USE DATABASE customer360;"
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer wants to create a new table containing the names of customers that live in France. They have written the following command: A senior data engineer mentions that it is organization policy to include a table property indicating that the new table includes personally identifiable information (PII). Which of the following lines of code fills in the above blank to successfully complete the task?",
      "alternativas": [
        "A. There is no way to indicate whether a table contains PII.",
        "B. \"COMMENT PII\"",
        "C. TBLPROPERTIES PII",
        "D. COMMENT \"Contains PII\"",
        "E. PII"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following benefits is provided by the array functions from Spark SQL?",
      "alternativas": [
        "A. An ability to work with data in a variety of types at once",
        "B. An ability to work with data within certain partitions and windows",
        "C. An ability to work with time-related data in specified intervals",
        "D. An ability to work with complex, nested data ingested from JSON files",
        "E. An ability to work with an array of tables for procedural automation"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following commands can be used to write data into a Delta table while avoiding the writing of duplicate records?",
      "alternativas": [
        "A. DROP",
        "B. IGNORE",
        "C. MERGE",
        "D. APPEND",
        "E. INSERT"
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer needs to apply custom logic to the string column `city` in table `stores` for a specific use case. In order to apply this custom logic at scale, the data engineer wants to create a SQL user-defined function (UDF). Which of the following code blocks creates this SQL UDF?",
      "alternativas": [
        "A. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;",
        "B. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING AS (CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END);",
        "C. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING BEGIN RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END; END;",
        "D. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING AS CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;",
        "E. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;"
      ],
      "answer": "E"
    },
    {
      "question": "A data analyst has a series of queries in a SQL program. The data analyst wants this program to run every day. They only want the final query in the program to run on Sundays. They ask for help from the data engineering team to complete this task. Which of the following approaches could be used by the data engineering team to complete this task?",
      "alternativas": [
        "A. They could submit a feature request with Databricks to add this functionality.",
        "B. They could wrap the queries using PySpark and use Python's control flow system to determine when to run the final query.",
        "C. They could only run the entire program on Sundays.",
        "D. They could automatically restrict access to the source table in the final query so that it is only accessible on Sundays.",
        "E. They could redesign the data model to separate the data used in the final query into a new table."
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer only wants to execute the final block of a Python program if the Python variable `day_of_week` is equal to `1` and the Python variable `review_period` is `True`. Which of the following control flow statements should the data engineer use to begin this conditionally executed code block?",
      "alternativas": [
        "A. if day_of_week == 1 and review_period:",
        "B. if day_of_week == 1 and review_period == \"True\":",
        "C. if day_of_week == 1 and review_period == True:",
        "D. if day_of_week == 1 & review_period:",
        "E. if (day_of_week == 1) and (review_period == True):"
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer is attempting to drop a Spark SQL table my_table. The data engineer wants to delete all table metadata and data. They run the following command: DROP TABLE IF EXISTS my_table. While the object no longer appears when they run SHOW TABLES, the data files still exist. Which of the following describes why the data files still exist and the metadata files were deleted?",
      "alternativas": [
        "A. The table's data was larger than 10 GB",
        "B. The table's data was smaller than 10 GB",
        "C. The table was external",
        "D. The table did not have a location",
        "E. The table was managed"
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer is attempting to drop a Spark SQL table my_table and runs the following command: DROP TABLE IF EXISTS my_table; After running this command, the engineer notices that the data files and metadata files have been deleted from the file system. Which of the following describes why all of these files were deleted?",
      "alternativas": [
        "A. The table was managed",
        "B. The table's data was smaller than 10 GB",
        "C. The table's data was larger than 10 GB",
        "D. The table was external",
        "E. The table did not have a location"
      ],
      "answer": "A"
    },
    {
      "question": "A data engineer wants to create a data entity from a couple of tables. The data entity must be used by other data engineers in other sessions. It also must be saved to a physical location. Which of the following data entities should the data engineer create?",
      "alternativas": [
        "A. Table",
        "B. Function",
        "C. View",
        "D. Temporary view"
      ],
      "answer": "A"
    },
    {
      "question": "A data engineer runs a statement every day to copy the previous day's sales into the table `transactions`. Each day's sales are in their own file in the location `/transactions/raw/`. Today, the data engineer runs the following command to complete this task: After running the command today, the data engineer notices that the number of records in table `transactions` has not changed. Which of the following describes why the statement might not have copied any new records into the table?",
      "alternativas": [
        "A. The format of the files to be copied were not included with the FORMAT_OPTIONS keyword.",
        "B. The names of the files to be copied were not included with the FILES keyword.",
        "C. The previous day's file has already been copied into the table.",
        "D. The PARQUET file format does not support COPY INTO.",
        "E. The COPY INTO statement requires the table to be refreshed to view the copied rows."
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer needs to create a table in Databricks using data from their organization's existing SQLite database. They run the following command: Which of the following lines of code fills in the above blank to successfully complete the task?",
      "alternativas": [
        "A. org.apache.spark.sql.jdbc",
        "B. autoloader",
        "C. DELTA",
        "D. sqlite",
        "E. org.apache.spark.sql.sqlite"
      ],
      "answer": "A"
    },
    {
      "question": "A data engineering team has two tables. The first table march_transactions is a collection of all retail transactions in the month of March. The second table april_transactions is a collection of all retail transactions in the month of April. There are no duplicate records between the tables. Which of the following commands should be run to create a new table all_transactions that contains all records from march_transactions and april_transactions without duplicate records?",
      "alternativas": [
        "A. CREATE TABLE all_transactions AS SELECT * FROM march_transactions INNER JOIN SELECT * FROM april_transactions;",
        "B. CREATE TABLE all_transactions AS SELECT * FROM march_transactions UNION SELECT * FROM april_transactions;",
        "C. CREATE TABLE all_transactions AS SELECT * FROM march_transactions OUTER JOIN SELECT * FROM april_transactions;",
        "D. CREATE TABLE all_transactions AS SELECT * FROM march_transactions INTERSECT SELECT * from april_transactions;",
        "E. CREATE TABLE all_transactions AS SELECT * FROM march_transactions MERGE SELECT * FROM april_transactions;"
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer is maintaining a data pipeline. Upon data ingestion, the data engineer notices that the source data is starting to have a lower level of quality. The data engineer would like to automate the process of monitoring the quality level. Which of the following tools can the data engineer use to solve this problem?",
      "alternativas": [
        "A. Auto Loader",
        "B. Unity Catalog",
        "C. Delta Lake",
        "D. Delta Live Tables"
      ],
      "answer": "D"
    },
    {
      "question": "A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are defined against Delta Lake table sources using LIVE TABLE. The table is configured to run in Production mode using the Continuous Pipeline Mode. What is the expected outcome after clicking Start to update the pipeline assuming previously unprocessed data exists and all definitions are valid?",
      "alternativas": [
        "A. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist to allow for additional testing.",
        "B. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing.",
        "C. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will be deployed for the update and terminated when the pipeline is stopped.",
        "D. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated."
      ],
      "answer": "C"
    },
    {
      "question": "In order for Structured Streaming to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or reprocessing, which of the following two approaches is used by Spark to record the offset range of the data being processed in each trigger?",
      "alternativas": [
        "A. Checkpointing and Write-ahead Logs",
        "B. Structured Streaming cannot record the offset range of the data being processed in each trigger.",
        "C. Replayable Sources and Idempotent Sinks",
        "D. Write-ahead Logs and Idempotent Sinks",
        "E. Checkpointing and Idempotent Sinks"
      ],
      "answer": "A"
    },
    {
      "question": "Which of the following describes the relationship between Gold tables and Silver tables?",
      "alternativas": [
        "A. Gold tables are more likely to contain aggregations than Silver tables.",
        "B. Gold tables are more likely to contain valuable data than Silver tables.",
        "C. Gold tables are more likely to contain a less refined view of data than Silver tables.",
        "D. Gold tables are more likely to contain truthful data than Silver tables."
      ],
      "answer": "A"
    },
    {
      "question": "Which of the following describes the relationship between Bronze tables and raw data?",
      "alternativas": [
        "A. Bronze tables contain less data than raw data files.",
        "B. Bronze tables contain more truthful data than raw data.",
        "C. Bronze tables contain aggregates while raw data is unaggregated.",
        "D. Bronze tables contain a less refined view of data than raw data.",
        "E. Bronze tables contain raw data with a schema applied."
      ],
      "answer": "E"
    },
    {
      "question": "Which of the following tools is used by Auto Loader to process data incrementally?",
      "alternativas": [
        "A. Checkpointing",
        "B. Spark Structured Streaming",
        "C. Data Explorer",
        "D. Unity Catalog",
        "E. Databricks SQL"
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table. Which line of code should the data engineer use to fill in the blank if the data engineer only wants the query to execute a micro-batch to process data every 5 seconds?",
      "alternativas": [
        "A. trigger(\"5 seconds\")",
        "B. trigger(continuous=\"5 seconds\")",
        "C. trigger(once=\"5 seconds\")",
        "D. trigger(processingTime=\"5 seconds\")"
      ],
      "answer": "D"
    },
    {
      "question": "A dataset has been defined using Delta Live Tables and includes an expectations clause: CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW. What is the expected behavior when a batch of data containing data that violates these constraints is processed?",
      "alternativas": [
        "A. Records that violate the expectation cause the job to fail.",
        "B. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target dataset.",
        "C. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.",
        "D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log."
      ],
      "answer": "C"
    },
    {
      "question": "A dataset has been defined using Delta Live Tables and includes an expectations clause: CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION FAIL UPDATE. What is the expected behavior when a batch of data containing data that violates these constraints is processed?",
      "alternativas": [
        "A. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.",
        "B. Records that violate the expectation cause the job to fail.",
        "C. Records that violate the expectation are dropped from the target dataset and loaded into a quarantine table.",
        "D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.",
        "E. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target dataset."
      ],
      "answer": "B"
    },
    {
      "question": "Which of the following describes when to use the CREATE STREAMING LIVE TABLE syntax over the CREATE LIVE TABLE syntax when creating Delta Live Tables (DLT) tables using SQL?",
      "alternativas": [
        "A. CREATE STREAMING LIVE TABLE should be used when the subsequent step in the DLT pipeline is static.",
        "B. CREATE STREAMING LIVE TABLE should be used when data needs to be processed incrementally.",
        "C. CREATE STREAMING LIVE TABLE should be used when data needs to be processed through complicated aggregations.",
        "D. CREATE STREAMING LIVE TABLE should be used when the previous step in the DLT pipeline is static."
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer is designing a data pipeline. The source system generates files in a shared directory that is also used by other processes. As a result, the files should be kept as is and will accumulate in the directory. The data engineer needs to identify which files are new since the previous run in the pipeline, and set up the pipeline to only ingest those new files with each run. Which of the following tools can the data engineer use to solve this problem?",
      "alternativas": [
        "A. Unity Catalog",
        "B. Delta Lake",
        "C. Databricks SQL",
        "D. Data Explorer",
        "E. Auto Loader"
      ],
      "answer": "E"
    },
    {
      "question": "A data engineer has three tables in a Delta Live Tables (DLT) pipeline. They have configured the pipeline to drop invalid records at each table. They notice that some data is being dropped due to quality concerns at some point in the DLT pipeline. They would like to determine at which table in their pipeline the data is being dropped. Which approach can the data engineer take to identify the table that is dropping the records?",
      "alternativas": [
        "A. They can set up separate expectations for each table when developing their DLT pipeline.",
        "B. They can navigate to the DLT pipeline page, click on the “Error” button, and review the present errors.",
        "C. They can set up DLT to notify them via email when records are dropped.",
        "D. They can navigate to the DLT pipeline page, click on each table, and view the data quality statistics."
      ],
      "answer": "D"
    },
    {
      "question": "A data engineer has a single-task Job that runs each morning before they begin working. After identifying an upstream data issue, they need to set up another task to run a new notebook prior to the original task. Which approach can the data engineer use to set up the new task?",
      "alternativas": [
        "A. They can clone the existing task in the existing Job and update it to run the new notebook.",
        "B. They can create a new task in the existing Job and then add it as a dependency of the original task.",
        "C. They can create a new task in the existing Job and then add the original task as a dependency of the new task.",
        "D. They can create a new job from scratch and add both tasks to run concurrently."
      ],
      "answer": "C"
    },
    {
      "question": "An engineering manager wants to monitor the performance of a recent project using a Databricks SQL query. For the first week following the project's release, the manager wants the query results to be updated every minute. However, the manager is concerned that the compute resources used for the query will be left running and cost the organization a lot of money beyond the first week of the project's release. Which approach can the engineering team use to ensure the query does not cost the organization any money beyond the first week of the project's release?",
      "alternativas": [
        "A. They can set a limit to the number of DBUs that are consumed by the SQL Endpoint.",
        "B. They can set the query's refresh schedule to end after a certain number of refreshes.",
        "C. They can set the query's refresh schedule to end on a certain date in the query scheduler.",
        "D. They can set a limit to the number of individuals that are able to manage the query's refresh schedule."
      ],
      "answer": "C"
    },
    {
      "question": "A data analysis team has noticed that their Databricks SQL queries are running too slowly when connected to their always-on SQL endpoint. They claim that this issue is present when many members of the team are running small queries simultaneously. They ask the data engineering team for help. The data engineering team notices that each of the team's queries uses the same SQL endpoint. Which approach can the data engineering team use to improve the latency of the team's queries?",
      "alternativas": [
        "A. They can increase the cluster size of the SQL endpoint.",
        "B. They can increase the maximum bound of the SQL endpoint's scaling range.",
        "C. They can turn on the Auto Stop feature for the SQL endpoint.",
        "D. They can turn on the Serverless feature for the SQL endpoint."
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer wants to schedule their Databricks SQL dashboard to refresh once per day, but they only want the associated SQL endpoint to be running when it is necessary. Which approach can the data engineer use to minimize the total running time of the SQL endpoint used in the refresh schedule of their dashboard?",
      "alternativas": [
        "A. They can ensure the dashboard's SQL endpoint matches each of the queries' SQL endpoints.",
        "B. They can set up the dashboard's SQL endpoint to be serverless.",
        "C. They can turn on the Auto Stop feature for the SQL endpoint.",
        "D. They can ensure the dashboard's SQL endpoint is not one of the included query's SQL endpoint."
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to an ELT job. The ELT job has its Databricks SQL query that returns the number of input records containing unexpected NULL values. The data engineer wants their entire team to be notified via a messaging webhook whenever this value reaches 100. Which approach can the data engineer use to notify their entire team via a messaging webhook whenever the number of NULL values reaches 100?",
      "alternativas": [
        "A. They can set up an Alert with a custom template.",
        "B. They can set up an Alert with a new email alert destination.",
        "C. They can set up an Alert with a new webhook alert destination.",
        "D. They can set up an Alert with one-time notifications."
      ],
      "answer": "C"
    },
    {
      "question": "A single Job runs two notebooks as two separate tasks. A data engineer has noticed that one of the notebooks is running slowly in the Job's current run. The data engineer asks a tech lead for help in identifying why this might be the case. Which approach can the tech lead use to identify why the notebook is running slowly as part of the Job?",
      "alternativas": [
        "A. They can navigate to the Runs tab in the Jobs UI to immediately review the processing notebook.",
        "B. They can navigate to the Tasks tab in the Jobs UI and click on the active run to review the processing notebook.",
        "C. They can navigate to the Runs tab in the Jobs UI and click on the active run to review the processing notebook.",
        "D. They can navigate to the Tasks tab in the Jobs UI to immediately review the processing notebook."
      ],
      "answer": "C"
    },
    {
      "question": "A data engineer has a Job with multiple tasks that runs nightly. Each of the tasks runs slowly because the clusters take a long time to start. Which action can the data engineer perform to improve the start up time for the clusters used for the Job?",
      "alternativas": [
        "A. They can use endpoints available in Databricks SQL",
        "B. They can use job clusters instead of all-purpose clusters",
        "C. They can configure the clusters to autoscale for larger data sizes",
        "D. They can use clusters that are from a cluster pool"
      ],
      "answer": "B"
    },
    {
      "question": "A new data engineering team has been assigned to an ELT project. The new data engineering team will need full privileges on the database customers to fully manage the project. Which of the following commands can be used to grant full permissions on the database to the new data engineering team?",
      "alternativas": [
        "A. GRANT USAGE ON DATABASE customers TO team;",
        "B. GRANT ALL PRIVILEGES ON DATABASE team TO customers;",
        "C. GRANT SELECT PRIVILEGES ON DATABASE customers TO teams;",
        "D. GRANT SELECT CREATE MODIFY USAGE PRIVILEGES ON DATABASE customers TO team;",
        "E. GRANT ALL PRIVILEGES ON DATABASE customers TO team;"
      ],
      "answer": "E"
    },
    {
      "question": "A new data engineering team has been assigned to work on a project. The team will need access to database customers in order to see what tables already exist. The team has its own group team. Which command can be used to grant the necessary permission on the entire database to the new team?",
      "alternativas": [
        "A. GRANT VIEW ON CATALOG customers TO team;",
        "B. GRANT CREATE ON DATABASE customers TO team;",
        "C. GRANT USAGE ON CATALOG team TO customers;",
        "D. GRANT USAGE ON DATABASE customers TO team;"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following is a benefit of the Databricks Lakehouse Platform embracing open source technologies?",
      "alternativas": [
        "A. Cloud-specific integrations",
        "B. Simplified governance",
        "C. Ability to scale storage",
        "D. Ability to scale workloads",
        "E. Avoiding vendor lock-in"
      ],
      "answer": "E"
    },
    {
      "question": "A data engineer needs to use a Delta table as part of a data pipeline, but they do not know if they have the appropriate permissions. In which of the following locations can the data engineer review their permissions on the table?",
      "alternativas": [
        "A. Databricks Filesystem",
        "B. Jobs",
        "C. Dashboards",
        "D. Repos",
        "E. Data Explorer"
      ],
      "answer": "E"
    },
    {
      "question": "A data engineer has been given a new record of data: id STRING = 'a1' rank INTEGER = 6 rating FLOAT = 9.4. Which of the following SQL commands can be used to append the new record to an existing Delta table my_table?",
      "alternativas": [
        "A. INSERT INTO my_table VALUES ('a1', 6, 9.4)",
        "B. my_table UNION VALUES ('a1', 6, 9.4)",
        "C. INSERT VALUES ( 'a1' , 6, 9.4) INTO my_table",
        "D. UPDATE my_table VALUES ('a1', 6, 9.4)",
        "E. UPDATE VALUES ('a1', 9.4) my_table"
      ],
      "answer": "A"
    },
    {
      "question": "Which of the following describes a scenario in which a data engineer will want to use a single-node cluster?",
      "alternativas": [
        "A. When they are working interactively with a small amount of data",
        "B. When they are running automated reports to be refreshed as quickly as possible",
        "C. When they are working with SQL within Databricks SQL",
        "D. When they are concerned about the ability to automatically scale with larger data",
        "E. When they are manually running reports with a large amount of data"
      ],
      "answer": "A"
    },
    {
      "question": "A data engineer has realized that the data files associated with a Delta table are incredibly small. They want to compact the small files to form larger files to improve performance. Which of the following keywords can be used to compact the small files?",
      "alternativas": [
        "A. REDUCE",
        "B. OPTIMIZE",
        "C. COMPACTION",
        "D. REPARTITION",
        "E. VACUUM"
      ],
      "answer": "B"
    },
    {
      "question": "In which of the following file formats is data from Delta Lake tables primarily stored?",
      "alternativas": [
        "A. Delta",
        "B. CSV",
        "C. Parquet",
        "D. JSON",
        "E. A proprietary, optimized format specific to Databricks"
      ],
      "answer": "C"
    },
    {
      "question": "Which of the following is stored in the Databricks customer's cloud account?",
      "alternativas": [
        "A. Databricks web application",
        "B. Cluster management metadata",
        "C. Repos",
        "D. Data",
        "E. Notebooks"
      ],
      "answer": "D"
    },
    {
      "question": "Which of the following can be used to simplify and unify siloed data architectures that are specialized for specific use cases?",
      "alternativas": [
        "A. None of these",
        "B. Data lake",
        "C. Data warehouse",
        "D. All of these",
        "E. Data lakehouse"
      ],
      "answer": "E"
    },
    {
      "question": "A data architect has determined that a table with the following format is necessary: employeeId | startDate | avgRating. Which of the following code blocks uses SQL DDL commands to create an empty Delta table in this format, regardless of whether a table already exists with this name?",
      "alternativas": [
        "A. CREATE TABLE IF NOT EXISTS table_name ( employeeId STRING, startDate DATE, avgRating FLOAT );",
        "B. CREATE OR REPLACE TABLE table_name AS SELECT employeeId STRING, startDate DATE, avgRating FLOAT USING DELTA;",
        "C. CREATE OR REPLACE TABLE table_name WITH COLUMNS ( employeeId STRING, startDate DATE, avgRating FLOAT ) USING DELTA;",
        "D. CREATE TABLE table_name AS SELECT employeeId STRING, startDate DATE, avgRating FLOAT;",
        "E. CREATE OR REPLACE TABLE table_name ( employeeId STRING, startDate DATE, avgRating FLOAT );"
      ],
      "answer": "E"
    },
    {
      "question": "A data engineer has a Python notebook in Databricks, but they need to use SQL to accomplish a specific task within a cell. They still want all of the other cells to use Python without making any changes to those cells. Which of the following describes how the data engineer can use SQL within a cell of their Python notebook?",
      "alternativas": [
        "A. It is not possible to use SQL in a Python notebook",
        "B. They can attach the cell to a SQL endpoint rather than a Databricks cluster",
        "C. They can simply write SQL syntax in the cell",
        "D. They can add %sql to the first line of the cell",
        "E. They can change the default language of the notebook to SQL"
      ],
      "answer": "D"
    },
    {
      "question": "Which SQL keyword can be used to convert a table from a long format to a wide format?",
      "alternativas": [
        "A. TRANSFORM",
        "B. PIVOT",
        "C. SUM",
        "D. CONVERT"
      ],
      "answer": "B"
    },
    {
      "question": "A data engineer has a Python variable table_name that they would like to use in a SQL query. They want to construct a Python code block that will run the query using table_name. They have the following incomplete code block: ____(f\"SELECT customer_id, spend FROM {table_name}\"). What can be used to fill in the blank to successfully complete the task?",
      "alternativas": [
        "A. spark.delta.sql",
        "B. spark.sql",
        "C. spark.table",
        "D. dbutils.sql"
      ],
      "answer": "B"
    },
    {
      "question": "What is used by Spark to record the offset range of the data being processed in each trigger in order for Structured Streaming to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or reprocessing?",
      "alternativas": [
        "A. Checkpointing and Write-ahead Logs",
        "B. Replayable Sources and Idempotent Sinks",
        "C. Write-ahead Logs and Idempotent Sinks",
        "D. Checkpointing and Idempotent Sinks"
      ],
      "answer": "A"
    },
    {
      "question": "A data engineer has developed a data pipeline to ingest data from a JSON source using Auto Loader, but the engineer has not provided any type inference or schema hints in their pipeline. Upon reviewing the data, the data engineer has noticed that all of the columns in the target table are of the string type despite some of the fields only including float or boolean values. Why has Auto Loader inferred all of the columns to be of the string type?",
      "alternativas": [
        "A. Auto Loader cannot infer the schema of ingested data",
        "B. JSON data is a text-based format",
        "C. Auto Loader only works with string data",
        "D. All of the fields had at least one null value"
      ],
      "answer": "B"
    },
    {
      "question": "Which statement regarding the relationship between Silver tables and Bronze tables is always true?",
      "alternativas": [
        "A. Silver tables contain a less refined, less clean view of data than Bronze data.",
        "B. Silver tables contain aggregates while Bronze data is unaggregated.",
        "C. Silver tables contain more data than Bronze tables.",
        "D. Silver tables contain less data than Bronze tables."
      ],
      "answer": "D"
    },
    {
      "question": "Which query is performing a streaming hop from raw data to a Bronze table?",
      "alternativas": [
        "A. (spark.table(\"sales\").groupBy(\"store\").agg(sum(\"sales\")).writeStream.option(\"checkpointLocation\", checkpointPath).outputMode(\"complete\").table(\"newSales\"))",
        "B. (spark.read.load(rawSalesLocation).write.mode(\"append\").table(\"newSales\"))",
        "C. (spark.table(\"sales\").withColumn(\"avgPrice\", col(\"sales\") / col(\"units\")).writeStream.option(\"checkpointLocation\", checkpointPath).outputMode(\"append\").table(\"newSales\"))",
        "D. (spark.readStream.load(rawSalesLocation).writeStream.option(\"checkpointLocation\", checkpointPath).outputMode(\"append\").table(\"newSales\"))"
      ],
      "answer": "D"
    },
    {
      "question": "You need to load a Bronze (Delta) table daily from new files in cloud storage, ensuring idempotent reprocessing and avoiding duplicates. Which approach is the most appropriate?",
      "alternativas": [
        "A. INSERT OVERWRITE daily on the Bronze table",
        "B. COPY INTO with file tracking + MERGE on the Silver table by natural key",
        "C. VACUUM before each load",
        "D. OPTIMIZE before each load"
      ],
      "answer": "B"
    },
    {
      "question": "You have a stream arriving in Delta and need to ensure exactly-once semantics on write. What is most important to configure?",
      "alternativas": [
        "A. Only trigger(once=True)",
        "B. Only repartition() before write",
        "C. Stable checkpoint location + transactional sink (Delta)",
        "D. cache() on the DataFrame"
      ],
      "answer": "C"
    },
    {
      "question": "A nested JSON arrives with dozens of fields and frequent changes. You want to control schema evolution without breaking downstream. Best option:",
      "alternativas": [
        "A. Infer schema always and write everything automatically",
        "B. Define schema explicitly and version the contract; allow controlled evolution when necessary",
        "C. Convert everything to CSV before persisting",
        "D. Use only temporary views"
      ],
      "answer": "B"
    },
    {
      "question": "You receive events with possible duplication (same event_id). Which pattern is the most robust before the MERGE?",
      "alternativas": [
        "A. dropDuplicates(['event_id']) without ordering",
        "B. Window with row_number() by event_id ordered by ingestion_time desc and filter rn=1",
        "C. global distinct()",
        "D. coalesce(1) and then MERGE"
      ],
      "answer": "B"
    },
    {
      "question": "Your Delta table has thousands of small files and queries have become slow. The most appropriate action:",
      "alternativas": [
        "A. VACUUM 0 HOURS",
        "B. OPTIMIZE to compact files (and consider ZORDER when it makes sense)",
        "C. CACHE TABLE permanently",
        "D. REFRESH TABLE repeatedly"
      ],
      "answer": "B"
    },
    {
      "question": "When does Z-ORDER tend to help the most?",
      "alternativas": [
        "A. Full scan queries without filters",
        "B. Queries with selective recurring filters on specific columns",
        "C. Only on small tables",
        "D. On tables that are not Delta"
      ],
      "answer": "B"
    },
    {
      "question": "You need to ensure that a team sees only rows from their own country (RLS) and that sensitive columns are masked (CLM). Where is this implemented in the most governable way?",
      "alternativas": [
        "A. In each notebook via manual filters",
        "B. In the storage layer via folders per country",
        "C. In Unity Catalog, with governance policies/resources (e.g., views/policies for RLS and masking)",
        "D. On the cluster, via init script"
      ],
      "answer": "C"
    },
    {
      "question": "A user can open a notebook but cannot run it on a shared cluster. The most likely cause:",
      "alternativas": [
        "A. Missing Can Read permission on the workspace",
        "B. Missing permission to use the cluster (e.g., Can Attach To or equivalent depending on configuration)",
        "C. Missing permission on the Git repo",
        "D. Missing permission to create jobs"
      ],
      "answer": "B"
    },
    {
      "question": "You have a pipeline with 5 tasks and want to avoid costs when a task fails, as well as ensure controlled re-runs. Best practice:",
      "alternativas": [
        "A. Run everything in a single monolithic notebook",
        "B. Break into tasks, use dependencies and retry/timeout policies per task",
        "C. Run only manually",
        "D. Remove alerts to reduce noise"
      ],
      "answer": "B"
    },
    {
      "question": "You want to apply quality rules and automatically track violations in a declarative pipeline. Best resource:",
      "alternativas": [
        "A. VACUUM",
        "B. EXPECTATIONS in DLT with quality metrics",
        "C. OPTIMIZE ZORDER",
        "D. DESCRIBE HISTORY"
      ],
      "answer": "B"
    },
    {
      "question": "For analytical consumption and BI, which modeling tends to be preferred in the Gold layer?",
      "alternativas": [
        "A. Only 3NF in all layers",
        "B. Data Vault only",
        "C. Dimensional model (fact/dimensions) in the Gold layer",
        "D. Nested JSON in the Gold layer"
      ],
      "answer": "C"
    },
    {
      "question": "You need to maintain a history of attribute changes (e.g., customer address). Which strategy is most aligned?",
      "alternativas": [
        "A. Update existing row (Type 1)",
        "B. Create an event table and never update anything",
        "C. SCD Type 2 with MERGE managing valid_from/valid_to/is_current",
        "D. Save only the latest version in a view"
      ],
      "answer": "C"
    },
    {
      "question": "You need to share data with external consumers without exposing storage credentials or duplicating data. Most appropriate option:",
      "alternativas": [
        "A. Send CSV by email",
        "B. Manually create a copy of the table in another workspace",
        "C. Delta Sharing (when applicable)",
        "D. Export to Parquet and upload to FTP"
      ],
      "answer": "C"
    },
    {
      "question": "To investigate slowness in a Spark job, which sequence makes the most sense?",
      "alternativas": [
        "A. Ignore UI and just increase the cluster",
        "B. View Spark UI (stages/tasks/shuffle) and execution metrics; identify skew/shuffles and adjustments",
        "C. Only run VACUUM",
        "D. Only run REFRESH TABLE"
      ],
      "answer": "B"
    },
    {
      "question": "Your job runs well but is expensive. Which action usually brings real gain without changing semantics?",
      "alternativas": [
        "A. Always increase the number of workers",
        "B. Adjust parallelism/partitioning, avoid unnecessary shuffles, choose appropriate instances and autoscaling with limits",
        "C. Disable checkpoints",
        "D. Convert Delta to CSV"
      ],
      "answer": "B"
    },
    {
      "question": "You need to use API credentials in notebooks. Best practice:",
      "alternativas": [
        "A. Paste the token in the notebook",
        "B. Store in driver environment variable and commit to repo",
        "C. Use Secret Scope/secret manager and reference via dbutils.secrets (or equivalent mechanism)",
        "D. Save in a Delta table"
      ],
      "answer": "C"
    },
    {
      "question": "You want to promote notebooks/pipelines between dev → qa → prod with traceability. Best approach:",
      "alternativas": [
        "A. Manual copy/paste in the workspace",
        "B. Use version control + automation (CI/CD) and parameterization per environment",
        "C. Keep environments the same and edit in production",
        "D. Run only locally"
      ],
      "answer": "B"
    },
    {
      "question": "What is the most useful test for a data pipeline?",
      "alternativas": [
        "A. Test only if the notebook ran without error",
        "B. Quality tests: counts, unique keys, null checks, business rules, samples and reconciliation",
        "C. Only test performance with cache()",
        "D. Only test workspace permissions"
      ],
      "answer": "B"
    },
    {
      "question": "A report broke because someone made an incorrect update yesterday. You want to quickly recover the data to the previous state. Which resource helps?",
      "alternativas": [
        "A. OPTIMIZE",
        "B. DESCRIBE HISTORY + Time Travel (query by version/timestamp) to restore/validate",
        "C. VACUUM",
        "D. REPARTITION"
      ],
      "answer": "B"
    },
    {
      "question": "Regarding VACUUM in Delta, which statement is most correct?",
      "alternativas": [
        "A. It should always be run with 0 hours retention in production",
        "B. It removes old versions and unreferenced files, but retention/security must be respected to not break time travel/reprocessing",
        "C. It replaces OPTIMIZE",
        "D. It is mandatory before each MERGE"
      ],
      "answer": "B"
    },
    {
      "question": "What is a main advantage of Auto Loader compared to traditional spark.read?",
      "alternativas": [
        "A. Faster CSV reading",
        "B. Exclusive support for JSON",
        "C. Scalability and efficient incremental ingestion",
        "D. Eliminates the need for Delta"
      ],
      "answer": "C"
    },
    {
      "question": "In Auto Loader, the File Notification mode is preferred when:",
      "alternativas": [
        "A. There are few files",
        "B. The storage does not support events",
        "C. There are millions of new files frequently",
        "D. There is no checkpoint"
      ],
      "answer": "C"
    },
    {
      "question": "To ensure idempotent ingestion in Delta, the most correct pattern is:",
      "alternativas": [
        "A. INSERT OVERWRITE",
        "B. DELETE + INSERT",
        "C. MERGE based on business key",
        "D. DROP TABLE"
      ],
      "answer": "C"
    },
    {
      "question": "What does the checkpoint ensure in Structured Streaming?",
      "alternativas": [
        "A. File compaction",
        "B. Schema control",
        "C. State recovery and exactly-once",
        "D. Best parallelism"
      ],
      "answer": "C"
    },
    {
      "question": "Which command helps investigate past changes in a Delta table?",
      "alternativas": [
        "A. OPTIMIZE",
        "B. DESCRIBE HISTORY",
        "C. VACUUM",
        "D. REFRESH"
      ],
      "answer": "B"
    },
    {
      "question": "VACUUM removes:",
      "alternativas": [
        "A. Current data",
        "B. Catalog metadata",
        "C. Unreferenced files and old versions",
        "D. Query statistics"
      ],
      "answer": "C"
    },
    {
      "question": "Which resource allows restoring data to a previous state?",
      "alternativas": [
        "A. Photon",
        "B. Time Travel",
        "C. Auto Loader",
        "D. Federation"
      ],
      "answer": "B"
    },
    {
      "question": "Which problem does OPTIMIZE solve?",
      "alternativas": [
        "A. Duplicated data",
        "B. Small files",
        "C. Lack of schema",
        "D. Lack of permissions"
      ],
      "answer": "B"
    },
    {
      "question": "What is the risk of running VACUUM RETAIN 0 HOURS in production?",
      "alternativas": [
        "A. Increase cost",
        "B. Break Time Travel and reprocessing",
        "C. Duplicate data",
        "D. Increase query latency"
      ],
      "answer": "B"
    },
    {
      "question": "Databricks Workflows allow:",
      "alternativas": [
        "A. Only manual execution",
        "B. Orchestration with dependencies between tasks",
        "C. Only SQL",
        "D. Only all-purpose clusters"
      ],
      "answer": "B"
    },
    {
      "question": "What is a best practice in Workflows?",
      "alternativas": [
        "A. One giant notebook",
        "B. Small and reusable tasks",
        "C. No retry",
        "D. Always-on fixed cluster"
      ],
      "answer": "B"
    },
    {
      "question": "If a task fails in a workflow:",
      "alternativas": [
        "A. The entire job needs to be recreated",
        "B. Only the task can be re-executed",
        "C. Data is lost",
        "D. The cluster is deleted"
      ],
      "answer": "B"
    },
    {
      "question": "Where to analyze shuffle, skew, and stages?",
      "alternativas": [
        "A. Unity Catalog",
        "B. Spark UI",
        "C. DLT UI",
        "D. Git"
      ],
      "answer": "B"
    },
    {
      "question": "For pipeline failure alerts, you use:",
      "alternativas": [
        "A. VACUUM",
        "B. Workflow notifications",
        "C. OPTIMIZE",
        "D. Z-ORDER"
      ],
      "answer": "B"
    },
    {
      "question": "Which practice reduces costs in Jobs?",
      "alternativas": [
        "A. Always-on clusters",
        "B. Autoscaling + auto-termination",
        "C. More fixed workers",
        "D. Permanent cache"
      ],
      "answer": "B"
    },
    {
      "question": "Which metric is NOT typical for observability?",
      "alternativas": [
        "A. Execution time",
        "B. Processed volume",
        "C. Number of notebooks",
        "D. Failures"
      ],
      "answer": "C"
    },
    {
      "question": "To investigate cost per job, you use:",
      "alternativas": [
        "A. OPTIMIZE",
        "B. System Tables",
        "C. Photon",
        "D. Auto Loader"
      ],
      "answer": "B"
    },
    {
      "question": "Which advantage of serverless jobs?",
      "alternativas": [
        "A. Less governance",
        "B. Automatic compute management",
        "C. Less security",
        "D. Does not use Delta"
      ],
      "answer": "B"
    },
    {
      "question": "Which common error in jobs?",
      "alternativas": [
        "A. Parameters per environment",
        "B. Hardcoded paths and secrets",
        "C. Use of retries",
        "D. Structured logs"
      ],
      "answer": "B"
    },
    {
      "question": "Unity Catalog centralizes:",
      "alternativas": [
        "A. Only storage",
        "B. Metadata, permissions, and lineage",
        "C. Only clusters",
        "D. Only notebooks"
      ],
      "answer": "B"
    },
    {
      "question": "Row-Level Security is applied:",
      "alternativas": [
        "A. In storage",
        "B. On the cluster",
        "C. In views/policies in the catalog",
        "D. In the notebook"
      ],
      "answer": "C"
    },
    {
      "question": "Column Masking is evaluated:",
      "alternativas": [
        "A. At ingestion time",
        "B. At query time",
        "C. In VACUUM",
        "D. In Auto Loader"
      ],
      "answer": "B"
    },
    {
      "question": "System Tables allow:",
      "alternativas": [
        "A. Create data",
        "B. Auditing and billing",
        "C. Streaming ingestion",
        "D. External sharing"
      ],
      "answer": "B"
    },
    {
      "question": "Which System Table helps with costs?",
      "alternativas": [
        "A. lineage",
        "B. billing.usage",
        "C. table_history",
        "D. expectations"
      ],
      "answer": "B"
    },
    {
      "question": "Data Lineage answers:",
      "alternativas": [
        "A. Who can access",
        "B. Who changed schema",
        "C. Impact of changes",
        "D. Performance"
      ],
      "answer": "C"
    },
    {
      "question": "Secrets should be stored:",
      "alternativas": [
        "A. In the notebook",
        "B. In Git",
        "C. In Secret Scopes",
        "D. In Delta tables"
      ],
      "answer": "C"
    },
    {
      "question": "Permissions in UC are:",
      "alternativas": [
        "A. Per notebook",
        "B. Per cluster",
        "C. Granular (catalog, schema, table)",
        "D. Only global"
      ],
      "answer": "C"
    },
    {
      "question": "Which is NOT a function of UC?",
      "alternativas": [
        "A. Governance",
        "B. Lineage",
        "C. Job execution",
        "D. Access control"
      ],
      "answer": "C"
    },
    {
      "question": "Data masking is used for:",
      "alternativas": [
        "A. Performance",
        "B. Compliance",
        "C. Ingestion",
        "D. Streaming"
      ],
      "answer": "B"
    },
    {
      "question": "Delta Sharing allows:",
      "alternativas": [
        "A. External write",
        "B. Share data without exposing storage",
        "C. Physical replication",
        "D. Automatic ETL"
      ],
      "answer": "B"
    },
    {
      "question": "Lakehouse Federation is used when:",
      "alternativas": [
        "A. You want to copy data",
        "B. You need to query external data without moving it",
        "C. You need streaming",
        "D. You need DLT"
      ],
      "answer": "B"
    },
    {
      "question": "Federation works via:",
      "alternativas": [
        "A. Python",
        "B. Spark Streaming",
        "C. SQL",
        "D. CSV"
      ],
      "answer": "C"
    },
    {
      "question": "Main difference between Federation and Delta Sharing:",
      "alternativas": [
        "A. Federation writes data",
        "B. Delta Sharing executes remote queries",
        "C. Federation queries remotely",
        "D. No difference"
      ],
      "answer": "C"
    },
    {
      "question": "Serverless SQL is ideal for:",
      "alternativas": [
        "A. Heavy ETL",
        "B. BI and ad-hoc queries",
        "C. Streaming",
        "D. ML training"
      ],
      "answer": "B"
    },
    {
      "question": "Photon improves:",
      "alternativas": [
        "A. Security",
        "B. SQL performance",
        "C. Ingestion",
        "D. Governance"
      ],
      "answer": "B"
    },
    {
      "question": "Does Photon require code changes?",
      "alternativas": [
        "A. Yes",
        "B. Only SQL",
        "C. No",
        "D. Only Python"
      ],
      "answer": "C"
    },
    {
      "question": "Federation avoids:",
      "alternativas": [
        "A. Compute cost",
        "B. Data duplication",
        "C. Governance",
        "D. Security"
      ],
      "answer": "B"
    },
    {
      "question": "Delta Sharing is:",
      "alternativas": [
        "A. Proprietary",
        "B. Open protocol",
        "C. Only Azure",
        "D. Only AWS"
      ],
      "answer": "B"
    },
    {
      "question": "Serverless is most suitable when:",
      "alternativas": [
        "A. You need full cluster control",
        "B. Workload is unpredictable",
        "C. Continuous heavy ETL",
        "D. Custom Spark configs"
      ],
      "answer": "B"
    },
    {
      "question": "Databricks Asset Bundles are used for:",
      "alternativas": [
        "A. Backup",
        "B. Version and promote assets",
        "C. Ingestion",
        "D. Share data"
      ],
      "answer": "B"
    },
    {
      "question": "Asset Bundles help mainly with:",
      "alternativas": [
        "A. SQL performance",
        "B. CI/CD",
        "C. Streaming",
        "D. Security"
      ],
      "answer": "B"
    },
    {
      "question": "An Asset Bundle can contain:",
      "alternativas": [
        "A. Only notebooks",
        "B. Only jobs",
        "C. Notebooks, jobs, pipelines, configs",
        "D. Only data"
      ],
      "answer": "C"
    },
    {
      "question": "Which format is used in Bundles?",
      "alternativas": [
        "A. XML",
        "B. YAML",
        "C. JSON",
        "D. CSV"
      ],
      "answer": "B"
    },
    {
      "question": "Clear advantage of Bundles:",
      "alternativas": [
        "A. Higher cost",
        "B. Manual deploy",
        "C. Standardization and reproducibility",
        "D. Less governance"
      ],
      "answer": "C"
    },
    {
      "question": "Bundles allow:",
      "alternativas": [
        "A. Hardcoded paths",
        "B. Parameterization per environment",
        "C. Ignore dev/qa/prod",
        "D. Only production"
      ],
      "answer": "B"
    },
    {
      "question": "Asset Bundles replace:",
      "alternativas": [
        "A. Unity Catalog",
        "B. Git",
        "C. Manual copy/paste",
        "D. Scripts manual"
      ],
      "answer": "C"
    },
    {
      "question": "A common error without Bundles is:",
      "alternativas": [
        "A. Versioning",
        "B. Drift between environments",
        "C. Parameterization",
        "D. Automation"
      ],
      "answer": "B"
    },
    {
      "question": "Asset Bundles integrate well with:",
      "alternativas": [
        "A. VACUUM",
        "B. Jenkins / GitHub Actions",
        "C. Photon",
        "D. Auto Loader"
      ],
      "answer": "B"
    },
    {
      "question": "For enterprise deploy, the ideal combination is:",
      "alternativas": [
        "A. Loose notebooks",
        "B. Asset Bundles + Git + CI/CD",
        "C. Only UI",
        "D. Only manual Jobs"
      ],
      "answer": "B"
    },
    {
      "question": "What is the main benefit of Auto Loader in environments with many files?",
      "alternativas": [
        "A. Better compression",
        "B. Mandatory automatic inference",
        "C. Scalability and efficient incremental ingestion",
        "D. Elimination of Delta Lake"
      ],
      "answer": "C"
    },
    {
      "question": "To avoid logical duplication in streaming, the correct pattern is:",
      "alternativas": [
        "A. Simple append",
        "B. Repartition",
        "C. Deduplication by key + checkpoint",
        "D. Trigger once"
      ],
      "answer": "C"
    },
    {
      "question": "The checkpointLocation is essential because:",
      "alternativas": [
        "A. Compacts files",
        "B. Maintains state and ensures correct reprocessing",
        "C. Improves schema inference",
        "D. Reduces storage cost"
      ],
      "answer": "B"
    },
    {
      "question": "A MERGE is preferable to INSERT OVERWRITE when:",
      "alternativas": [
        "A. The table is small",
        "B. There is a need for incremental updates",
        "C. The data is only append",
        "D. There is no business key"
      ],
      "answer": "B"
    },
    {
      "question": "Which command allows auditing changes in Delta?",
      "alternativas": [
        "A. VACUUM",
        "B. DESCRIBE HISTORY",
        "C. OPTIMIZE",
        "D. CACHE"
      ],
      "answer": "B"
    },
    {
      "question": "Z-ORDER is most effective when:",
      "alternativas": [
        "A. There are no filters",
        "B. There are recurring selective filters on the same columns",
        "C. The table is small",
        "D. The table is not Delta"
      ],
      "answer": "B"
    },
    {
      "question": "Small files negatively impact:",
      "alternativas": [
        "A. Security",
        "B. Read performance",
        "C. Governance",
        "D. Schema"
      ],
      "answer": "B"
    },
    {
      "question": "Which action resolves small files without changing data?",
      "alternativas": [
        "A. MERGE",
        "B. OPTIMIZE",
        "C. VACUUM",
        "D. Time Travel"
      ],
      "answer": "B"
    },
    {
      "question": "Time Travel directly depends on:",
      "alternativas": [
        "A. Photon",
        "B. Version retention",
        "C. Z-ORDER",
        "D. Cache"
      ],
      "answer": "B"
    },
    {
      "question": "The biggest risk of an aggressive VACUUM is:",
      "alternativas": [
        "A. High cost",
        "B. Loss of Time Travel",
        "C. Duplication",
        "D. Slowness"
      ],
      "answer": "B"
    },
    {
      "question": "A best practice in pipelines is:",
      "alternativas": [
        "A. One single notebook",
        "B. Small and reusable tasks",
        "C. No retries",
        "D. No logs"
      ],
      "answer": "B"
    },
    {
      "question": "If a task fails, the workflow:",
      "alternativas": [
        "A. Needs to be recreated",
        "B. Can re-run only the task",
        "C. Loses data",
        "D. Deletes the cluster"
      ],
      "answer": "B"
    },
    {
      "question": "Autoscaling mainly helps to:",
      "alternativas": [
        "A. Security",
        "B. Fixed performance",
        "C. Reduce costs",
        "D. Governance"
      ],
      "answer": "C"
    },
    {
      "question": "For intermittent and unpredictable jobs, the ideal is:",
      "alternativas": [
        "A. Fixed cluster",
        "B. Serverless",
        "C. Permanent cache",
        "D. Manual repartition"
      ],
      "answer": "B"
    },
    {
      "question": "To investigate slowness, the first step is:",
      "alternativas": [
        "A. Increase cluster",
        "B. Analyze Spark UI",
        "C. Run VACUUM",
        "D. Enable Photon"
      ],
      "answer": "B"
    },
    {
      "question": "Retry is useful when:",
      "alternativas": [
        "A. There are temporary failures",
        "B. There is logic error",
        "C. The data is invalid",
        "D. The schema changed"
      ],
      "answer": "A"
    },
    {
      "question": "Which practice increases cost without benefit?",
      "alternativas": [
        "A. Auto-termination",
        "B. Always-on clusters",
        "C. Autoscaling",
        "D. Dependent tasks"
      ],
      "answer": "B"
    },
    {
      "question": "Structured logs help mainly with:",
      "alternativas": [
        "A. Ingestion",
        "B. Observability",
        "C. Security",
        "D. Schema"
      ],
      "answer": "B"
    },
    {
      "question": "A well-designed job should prioritize:",
      "alternativas": [
        "A. Short code",
        "B. Reliability and re-run",
        "C. Fewer tables",
        "D. Fewer tasks"
      ],
      "answer": "B"
    },
    {
      "question": "Row-Level Security controls:",
      "alternativas": [
        "A. Columns",
        "B. Rows visible per user",
        "C. Schema",
        "D. Storage"
      ],
      "answer": "B"
    },
    {
      "question": "Column Masking is applied:",
      "alternativas": [
        "A. At ingestion",
        "B. At query time",
        "C. In VACUUM",
        "D. In OPTIMIZE"
      ],
      "answer": "B"
    },
    {
      "question": "System Tables are used for:",
      "alternativas": [
        "A. ETL",
        "B. Auditing and billing",
        "C. Streaming",
        "D. Sharing"
      ],
      "answer": "B"
    },
    {
      "question": "Which table helps analyze costs?",
      "alternativas": [
        "A. access.audit",
        "B. billing.usage",
        "C. lineage",
        "D. table_history"
      ],
      "answer": "B"
    },
    {
      "question": "Lineage mainly answers:",
      "alternativas": [
        "A. Who accessed",
        "B. Impact of changes",
        "C. Performance",
        "D. Cost"
      ],
      "answer": "B"
    },
    {
      "question": "Where should secrets be stored?",
      "alternativas": [
        "A. Notebook",
        "B. Git",
        "C. Secret Scopes",
        "D. Delta"
      ],
      "answer": "C"
    },
    {
      "question": "A common governance error is:",
      "alternativas": [
        "A. Centralized policies",
        "B. Filters in the notebook",
        "C. Masking",
        "D. Secure views"
      ],
      "answer": "B"
    },
    {
      "question": "For LGPD/GDPR, the most relevant is:",
      "alternativas": [
        "A. Photon",
        "B. Masking + RLS",
        "C. Auto Loader",
        "D. OPTIMIZE"
      ],
      "answer": "B"
    },
    {
      "question": "Unity Catalog does NOT:",
      "alternativas": [
        "A. Govern",
        "B. Audit",
        "C. Execute jobs",
        "D. Control access"
      ],
      "answer": "C"
    },
    {
      "question": "Lakehouse Federation is ideal when:",
      "alternativas": [
        "A. Want to copy data",
        "B. Want to query external data without moving it",
        "C. Need write",
        "D. Need streaming"
      ],
      "answer": "B"
    },
    {
      "question": "Federation executes queries via:",
      "alternativas": [
        "A. Python",
        "B. SQL",
        "C. REST",
        "D. CSV"
      ],
      "answer": "B"
    },
    {
      "question": "Main disadvantage of Federation:",
      "alternativas": [
        "A. Lack of governance",
        "B. Latency dependent on the source",
        "C. Lack of SQL",
        "D. Lack of security"
      ],
      "answer": "B"
    },
    {
      "question": "Serverless SQL is most suitable for:",
      "alternativas": [
        "A. Heavy ETL",
        "B. BI and ad-hoc queries",
        "C. Streaming",
        "D. ML training"
      ],
      "answer": "B"
    },
    {
      "question": "Photon mainly accelerates:",
      "alternativas": [
        "A. Python loops",
        "B. Analytical SQL",
        "C. Ingestion",
        "D. Streaming"
      ],
      "answer": "B"
    },
    {
      "question": "Does Photon require code changes?",
      "alternativas": [
        "A. Yes",
        "B. Only SQL",
        "C. No",
        "D. Only Python"
      ],
      "answer": "C"
    },
    {
      "question": "Correct tuning starts with:",
      "alternativas": [
        "A. More hardware",
        "B. Understanding the execution plan",
        "C. VACUUM",
        "D. Cache"
      ],
      "answer": "B"
    },
    {
      "question": "Bundles help mainly with:",
      "alternativas": [
        "A. Performance",
        "B. CI/CD",
        "C. Streaming",
        "D. Cost"
      ],
      "answer": "B"
    },
    {
      "question": "Bundles mainly use:",
      "alternativas": [
        "A. JSON",
        "B. YAML",
        "C. XML",
        "D. CSV"
      ],
      "answer": "B"
    },
    {
      "question": "Variables per environment allow:",
      "alternativas": [
        "A. Hardcode",
        "B. Reuse across dev/qa/prod",
        "C. Ignore governance",
        "D. Only production"
      ],
      "answer": "B"
    },
    {
      "question": "Bundles mainly avoid:",
      "alternativas": [
        "A. Git",
        "B. Drift between environments",
        "C. Logs",
        "D. Schema"
      ],
      "answer": "B"
    },
    {
      "question": "Rollback is facilitated because Bundles are:",
      "alternativas": [
        "A. Dynamic",
        "B. Versioned",
        "C. Serverless",
        "D. Temporary"
      ],
      "answer": "B"
    },
    {
      "question": "Ideal enterprise deploy combines:",
      "alternativas": [
        "A. Loose notebooks",
        "B. Bundles + Git + CI/CD + UC",
        "C. Only UI",
        "D. Only Jobs"
      ],
      "answer": "B"
    }
  ];