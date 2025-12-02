import streamlit as st
import random
import time

# Configuração da página para parecer mais um "app"
st.set_page_config(
    page_title="Simulado Databricks",
    page_icon="🟥",
    layout="centered"
)

# Estilos CSS customizados para melhorar a aparência mobile
st.markdown("""
    <style>
    .stButton>button {
        width: 100%;
        border-radius: 12px;
        height: 3em;
        font-weight: bold;
    }
    .stRadio>div {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 5rem;
    }
    div[data-testid="stProgressBar"] > div > div {
        background-color: #ff3621;
    }
    </style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# DADOS DAS PERGUNTAS (Baseado no seu JSON original)
# ---------------------------------------------------------
RAW_DATA = [
    {
        "pergunta": "A data organization leader is upset about the data analysis teams reports being different from the data \nengineering teams reports. The leader believes the siloed nature of their organizations data engineering and data analysis architectures \nis to blame. Which of the following describes how a data lakehouse could alleviate this issue?",
        "alternativas": [
            "A. Both teams would autoscale their work as data size evolves",
            "B. Both teams would use the same source of truth for their work",
            "C. Both teams would reorganize to report to the same department",
            "D. Both teams would be able to collaborate on projects in real-time",
            "E. Both teams would respond more quickly to ad-hoc requests"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which of the following describes a scenario in which a data team will want to utilize cluster pools?",
        "alternativas": [
            "A. An automated report needs to be refreshed as quickly as possible.",
            "B. An automated report needs to be made reproducible.",
            "C. An automated report needs to be tested to identify errors.",
            "D. An automated report needs to be version-controlled across multiple collaborators.",
            "E. An automated report needs to be runnable by all stakeholders."
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which of the following is hosted completely in the control plane of the classic Databricks architecture?",
        "alternativas": [
            "A. Worker node",
            "B. JDBC data source",
            "C. Databricks web application",
            "D. Databricks Filesystem",
            "E. Driver node"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following benefits of using the Databricks Lakehouse Platform is provided by Delta Lake?",
        "alternativas": [
            "A. The ability to manipulate the same data using a variety of languages",
            "B. The ability to collaborate in real time on a single notebook",
            "C. The ability to set up alerts for query failures",
            "D. The ability to support batch and streaming workloads",
            "E. The ability to distribute complex data operations"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following describes the storage organization of a Delta table?",
        "alternativas": [
            "A. Delta tables are stored in a single file that contains data, history, metadata, and other attributes.",
            "B. Delta tables store their data in a single file and all metadata in a collection of files in a separate location.",
            "C. Delta tables are stored in a collection of files that contain data, history, metadata, and other attributes.",
            "D. Delta tables are stored in a collection of files that contain only the data stored within the table.",
            "E. Delta tables are stored in a single file that contains only the data stored within the table."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following code blocks will remove the rows where the value in column age is greater than 25 from \nthe existing Delta table my_table and save the updated table?",
        "alternativas": [
            "A. SELECT * FROM my_table WHERE age > 25;",
            "B. UPDATE my_table WHERE age > 25;",
            "C. DELETE FROM my_table WHERE age > 25;",
            "D. UPDATE my_table WHERE age <= 25;",
            "E. DELETE FROM my_table WHERE age <= 25;"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer has realized that they made a mistake when making a daily update to a table. They need to use \nDelta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the \nolder version, they are unable to restore the data because the data files have been deleted. Which of the following explains why the data \nfiles are no longer present?",
        "alternativas": [
            "A. The VACUUM command was run on the table",
            "B. The TIME TRAVEL command was run on the table",
            "C. The DELETE HISTORY command was run on the table",
            "D. The OPTIMIZE command was nun on the table",
            "E. The HISTORY command was run on the table"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which of the following Git operations must be performed outside of Databricks Repos?",
        "alternativas": [
            "A. Commit",
            "B. Pull",
            "C. Push",
            "D. Clone",
            "E. Merge"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following data lakehouse features results in improved data quality over a traditional data lake?",
        "alternativas": [
            "A. A data lakehouse provides storage solutions for structured and unstructured data.",
            "B. A data lakehouse supports ACID-compliant transactions.",
            "C. A data lakehouse allows the use of SQL queries to examine data.",
            "D. A data lakehouse stores data in open formats.",
            "E. A data lakehouse enables machine learning and artificial Intelligence workloads."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer needs to determine whether to use the built-in Databricks Notebooks versioning or version their project using Databricks Repos. Which of the following is an advantage of using Databricks Repos over the Databricks Notebooks versioning?",
        "alternativas": [
            "A. Databricks Repos automatically saves development progress",
            "B. Databricks Repos supports the use of multiple branches",
            "C. Databricks Repos allows users to revert to previous versions of a notebook",
            "D. Databricks Repos provides the ability to comment on specific changes",
            "E. Databricks Repos is wholly housed within the Databricks Lakehouse Platform"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has left the organization. The data team needs to transfer ownership of the data engineers \nDelta tables to a new data engineer. The new data engineer is the lead engineer on the data team. Assuming the original data engineer \nno longer has access, which of the following individuals must be the one to transfer ownership of the Delta tables in Data Explorer? ",
        "alternativas": [
            "A. Databricks account representative ",
            "B. This transfer is not possible ",
            "C. Workspace administrator ",
            "D. New lead data engineer ",
            "E. Original data engineer "
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data analyst has created a Delta table sales that is used by the entire data analysis team. They want help from \nthe data engineering team to implement a series of tests to ensure the data is clean. However, the data engineering team uses Python \nfor its tests rather than SQL. Which of the following commands could the data engineering team use to access sales in PySpark?",
        "alternativas": [
            "A. SELECT * FROM sales",
            "B. There is no way to share data between PySpark and SQL. ",
            "C. spark.sql(\"sales\") ",
            "D. spark.delta.table(\"sales\") ",
            "E. spark.table(\"sales\") "
        ],
        "resposta": "E"
    },
    {
        "pergunta": "Which of the following commands will return the location of database customer360?",
        "alternativas": [
            "A. DESCRIBE LOCATION customer360;",
            "B. DROP DATABASE customer360;",
            "C. DESCRIBE DATABASE customer360;",
            "D. ALTER DATABASE customer360 SET DBPROPERTIES ('location' = '/user'};",
            "E. USE DATABASE customer360;"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer wants to create a new table containing the names of customers that live in France. \nThey have written the following command: \n \nA senior data engineer mentions that it is organization policy to include a table property indicating that the new table includes personally \nidentifiable information (PII). \nWhich of the following lines of code fills in the above blank to successfully complete the task?",
        "alternativas": [
            "A. There is no way to indicate whether a table contains PII.",
            "B. \"COMMENT PII\"",
            "C. TBLPROPERTIES PII",
            "D. COMMENT \"Contains PII\"",
            "E. PII"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following benefits is provided by the array functions from Spark SQL?",
        "alternativas": [
            "A. An ability to work with data in a variety of types at once",
            "B. An ability to work with data within certain partitions and windows",
            "C. An ability to work with time-related data in specified intervals",
            "D. An ability to work with complex, nested data ingested from JSON files",
            "E. An ability to work with an array of tables for procedural automation"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following commands can be used to write data into a Delta table while avoiding the writing of \nduplicate records?",
        "alternativas": [
            "A. DROP",
            "B. IGNORE",
            "C. MERGE",
            "D. APPEND",
            "E. INSERT"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which command can be used to write data into a Delta table while avoiding the writing of duplicate records?",
        "alternativas": [
            "A. DROP",
            "B. INSERT",
            "C. MERGE",
            "D. APPEND"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer needs to apply custom logic to the string column `city` in table `stores` for a specific use case. In order to apply this custom logic at scale, the data engineer wants to create a SQL user-defined function (UDF). Which of the following code blocks creates this SQL UDF?",
        "alternativas": [
            "A. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;",
            "B. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING AS (CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END);",
            "C. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING BEGIN RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END; END;",
            "D. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING AS CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;",
            "E. CREATE FUNCTION combine_nyc(city STRING) RETURNS STRING RETURN CASE WHEN city = 'brooklyn' THEN 'new york' ELSE city END;"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data analyst has a series of queries in a SQL program. The data analyst wants this program to run every day. \nThey only want the final query in the program to run on Sundays. They ask for help from the data engineering team to complete this task. \nWhich of the following approaches could be used by the data engineering team to complete this task?",
        "alternativas": [
            "A. They could submit a feature request with Databricks to add this functionality.",
            "B. They could wrap the queries using PySpark and use Pythons control flow system to determine when to run the final query.",
            "C. They could only run the entire program on Sundays.",
            "D. They could automatically restrict access to the source table in the final query so that it is only accessible on Sundays.",
            "E. They could redesign the data model to separate the data used in the final query into a new table."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer only wants to execute the final block of a Python program if the Python variable `day_of_week` is equal to `1` and the Python variable `review_period` is `True`. Which of the following control flow statements should the data engineer use to begin this conditionally executed code block?",
        "alternativas": [
            "A. if day_of_week == 1 and review_period:",
            "B. if day_of_week == 1 and review_period == \"True\":",
            "C. if day_of_week == 1 and review_period == True:",
            "D. if day_of_week == 1 & review_period:",
            "E. if (day_of_week == 1) and (review_period == True):"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer is attempting to drop a Spark SQL table my_table. The data engineer wants to delete all table \nmetadata and data. \nThey run the following command: \nDROP TABLE IF EXISTS my_table - \nWhile the object no longer appears when they run SHOW TABLES, the data files still exist. \nWhich of the following describes why the data files still exist and the metadata files were deleted?",
        "alternativas": [
            "A. The tables data was larger than 10 GB ",
            "B. The tables data was smaller than 10 GB ",
            "C. The table was external ",
            "D. The table did not have a location ",
            "E. The table was managed "
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer is attempting to drop a Spark SQL table my_table and runs the following command: DROP TABLE \nIF EXISTS my_table; After running this command, the engineer notices that the data files and metadata files have been deleted from the \nfile system. Which of the following describes why all of these files were deleted?",
        "alternativas": [
            "A. The table was managed",
            "B. The tables data was smaller than 10 GB",
            "C. The tables data was larger than 10 GB",
            "D. The table was external",
            "E. The table did not have a location"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer is attempting to drop a Spark SQL table my_table and runs the following command: \nDROP TABLE IF EXISTS my_table; \nAfter running this command, the engineer notices that the data files and metadata files have been deleted from the file system. What is \nthe reason behind the deletion of all these files?",
        "alternativas": [
            "A. The table was managed",
            "B. The tables data was smaller than 10 GB",
            "C. The table did not have a location",
            "D. The table was external"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer wants to create a data entity from a couple of tables. The data entity must be used by other \ndata engineers in other sessions. It also must be saved to a physical location. Which of the following data entities should the data \nengineer create?",
        "alternativas": [
            "A. Database",
            "B. Function",
            "C. View",
            "D. Temporary view",
            "E. Table"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer wants to create a data entity from a couple of tables. The data entity must be used by other \ndata engineers in other sessions. It also must be saved to a physical location. Which of the following data entities should the data \nengineer create?",
        "alternativas": [
            "A. Table",
            "B. Function",
            "C. View",
            "D. Temporary view"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer runs a statement every day to copy the previous days sales into the table `transactions`. Each days sales are in their own file in the location `/transactions/raw/`. \n\nToday, the data engineer runs the following command to complete this task:\n\nAfter running the command today, the data engineer notices that the number of records in table `transactions` has not changed. \n\nWhich of the following describes why the statement might not have copied any new records into the table?",
        "alternativas": [
            "A. The format of the files to be copied were not included with the FORMAT_OPTIONS keyword.",
            "B. The names of the files to be copied were not included with the FILES keyword.",
            "C. The previous days file has already been copied into the table.",
            "D. The PARQUET file format does not support COPY INTO.",
            "E. The COPY INTO statement requires the table to be refreshed to view the copied rows."
        ],
        "resposta": "C"
    },
    {
        "pergunta": " A data engineer needs to create a table in Databricks using data from their organizations existing SQLite \ndatabase. \nThey run the following command: \nWhich of the following lines of code fills in the above blank to successfully complete the task? ",
        "alternativas": [
            "A. org.apache.spark.sql.jdbc ",
            "B. autoloader ",
            "C. DELTA ",
            "D. sqlite ",
            "E. org.apache.spark.sql.sqlite "
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineering team has two tables. The first table march_transactions is a collection of all retail \ntransactions in the month of March. The second table april_transactions is a collection of all retail transactions in the month of April. \nThere are no duplicate records between the tables. \nWhich of the following commands should be run to create a new table all_transactions that contains all records from march_transactions \nand april_transactions without duplicate records?",
        "alternativas": [
            "A. CREATE TABLE all_transactions AS SELECT * FROM march_transactions INNER JOIN SELECT * FROM april_transactions;",
            "B. CREATE TABLE all_transactions AS SELECT * FROM march_transactions UNION SELECT * FROM april_transactions;",
            "C. CREATE TABLE all_transactions AS SELECT * FROM march_transactions OUTER JOIN SELECT * FROM april_transactions;",
            "D. CREATE TABLE all_transactions AS SELECT * FROM march_transactions INTERSECT SELECT * from april_transactions;",
            "E. CREATE TABLE all_transactions AS SELECT * FROM march_transactions MERGE SELECT * FROM april_transactions;"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer is maintaining a data pipeline. Upon data ingestion, the data engineer notices that the source \ndata is starting to have a lower level of quality. The data engineer would like to automate the process of monitoring the quality level. \nWhich of the following tools can the data engineer use to solve this problem?",
        "alternativas": [
            "A. Unity Catalog",
            "B. Data Explorer",
            "C. Delta Lake",
            "D. Delta Live Tables",
            "E. Auto Loader"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer is maintaining a data pipeline. Upon data ingestion, the data engineer notices that the source \ndata is starting to have a lower level of quality. The data engineer would like to automate the process of monitoring the quality level. \nWhich of the following tools can the data engineer use to solve this problem?",
        "alternativas": [
            "A. Auto Loader",
            "B. Unity Catalog",
            "C. Delta Lake",
            "D. Delta Live Tables"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are \ndefined against Delta Lake table sources using LIVE TABLE. The table is configured to run in Production mode using the Continuous Pipeline Mode. Assuming previously unprocessed data exists and all definitions are valid, what is the expected outcome after clicking Start to update the pipeline?",
        "alternativas": [
            "A. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist to allow for additional testing.",
            "B. All datasets will be updated once and the pipeline will persist without any processing. The compute resources will persist but go unused.",
            "C. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will be deployed for the update and terminated when the pipeline is stopped.",
            "D. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated.",
            "E. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are \ndefined against Delta Lake table sources using LIVE TABLE. The table is configured to run in Production mode using the Continuous \nPipeline Mode. What is the expected outcome after clicking Start to update the pipeline assuming previously unprocessed data exists and \nall definitions are valid?",
        "alternativas": [
            "A. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist to allow for additional testing.",
            "B. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing.",
            "C. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will be deployed for the update and terminated when the pipeline is stopped.",
            "D. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "In order for Structured Streaming to reliably track the exact progress of the processing so that it can handle any \nkind of failure by restarting and/or reprocessing, which of the following two approaches is used by Spark to record the offset range of the \ndata being processed in each trigger?",
        "alternativas": [
            "A. Checkpointing and Write-ahead Logs",
            "B. Structured Streaming cannot record the offset range of the data being processed in each trigger.",
            "C. Replayable Sources and Idempotent Sinks",
            "D. Write-ahead Logs and Idempotent Sinks",
            "E. Checkpointing and Idempotent Sinks"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which of the following describes the relationship between Gold tables and Silver tables?",
        "alternativas": [
            "A. Gold tables are more likely to contain aggregations than Silver tables.",
            "B. Gold tables are more likely to contain valuable data than Silver tables.",
            "C. Gold tables are more likely to contain a less refined view of data than Silver tables.",
            "D. Gold tables are more likely to contain more data than Silver tables.",
            "E. Gold tables are more likely to contain truthful data than Silver tables."
        ],
        "resposta": "A"
    },
    {
        "pergunta": "What describes the relationship between Gold tables and Silver tables?",
        "alternativas": [
            "A. Gold tables are more likely to contain aggregations than Silver tables.",
            "B. Gold tables are more likely to contain valuable data than Silver tables.",
            "C. Gold tables are more likely to contain a less refined view of data than Silver tables.",
            "D. Gold tables are more likely to contain truthful data than Silver tables."
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which of the following describes the relationship between Bronze tables and raw data?",
        "alternativas": [
            "A. Bronze tables contain less data than raw data files.",
            "B. Bronze tables contain more truthful data than raw data.",
            "C. Bronze tables contain aggregates while raw data is unaggregated.",
            "D. Bronze tables contain a less refined view of data than raw data.",
            "E. Bronze tables contain raw data with a schema applied."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following tools is used by Auto Loader process data incrementally?",
        "alternativas": [
            "A. Checkpointing",
            "B. Spark Structured Streaming",
            "C. Data Explorer",
            "D. Unity Catalog",
            "E. Databricks SQL"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table.\n\nThe code block used by the data engineer is below:\n\nWhich line of code should the data engineer use to fill in the blank if the data engineer only wants the query to execute a micro-batch to process data every 5 seconds?",
        "alternativas": [
            "A. trigger(\"5 seconds\")",
            "B. trigger(continuous=\"5 seconds\")",
            "C. trigger(once=\"5 seconds\")",
            "D. trigger(processingTime=\"5 seconds\")"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A dataset has been defined using Delta Live Tables and includes an expectations clause: \nCONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW \nWhat is the expected behavior when a batch of data containing data that violates these constraints is processed?",
        "alternativas": [
            "A. Records that violate the expectation are dropped from the target dataset and loaded into a quarantine table.",
            "B. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target \ndataset.",
            "C. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.",
            "D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.",
            "E. Records that violate the expectation cause the job to fail."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A dataset has been defined using Delta Live Tables and includes an expectations clause: \nCONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW \nWhat is the expected behavior when a batch of data containing data that violates these constraints is processed?",
        "alternativas": [
            "A. Records that violate the expectation cause the job to fail.",
            "B. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target \ndataset.",
            "C. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.",
            "D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A dataset has been defined using Delta Live Tables and includes an expectations clause: \nCONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION FAIL UPDATE \nWhat is the expected behavior when a batch of data containing data that violates these constraints is processed?",
        "alternativas": [
            "A. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.",
            "B. Records that violate the expectation cause the job to fail.",
            "C. Records that violate the expectation are dropped from the target dataset and loaded into a quarantine table.",
            "D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.",
            "E. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target \ndataset."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which of the following describes when to use the CREATE STREAMING LIVE TABLE (formerly CREATE \nINCREMENTAL LIVE TABLE) syntax over the CREATE LIVE TABLE syntax when creating Delta Live Tables (DLT) tables using SQL?",
        "alternativas": [
            "A. CREATE STREAMING LIVE TABLE should be used when the subsequent step in the DLT pipeline is static.",
            "B. CREATE STREAMING LIVE TABLE should be used when data needs to be processed incrementally.",
            "C. CREATE STREAMING LIVE TABLE is redundant for DLT and it does not need to be used.",
            "D. CREATE STREAMING LIVE TABLE should be used when data needs to be processed through complicated aggregations.",
            "E. CREATE STREAMING LIVE TABLE should be used when the previous step in the DLT pipeline is static."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer is designing a data pipeline. The source system generates files in a shared directory that is also \nused by other processes. As a result, the files should be kept as is and will accumulate in the directory. The data engineer needs to \nidentify which files are new since the previous run in the pipeline, and set up the pipeline to only ingest those new files with each run. \nWhich of the following tools can the data engineer use to solve this problem?",
        "alternativas": [
            "A. Unity Catalog",
            "B. Delta Lake",
            "C. Databricks SQL",
            "D. Data Explorer",
            "E. Auto Loader"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "Which of the following Structured Streaming queries is performing a hop from a Silver table to a Gold table? ",
        "alternativas": [
            "A. They can set up separate expectations for each table when developing their DLT pipeline.",
            "B. They cannot determine which table is dropping the records.",
            "C. They can set up DLT to notify them via email when records are dropped.",
            "D. They can navigate to the DLT pipeline page, click on each table, and view the data quality statistics.",
            "E. They can navigate to the DLT pipeline page, click on the “Error” button, and review the present errors."
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer has three tables in a Delta Live Tables (DLT) pipeline. They have configured the pipeline to drop \ninvalid records at each table. They notice that some data is being dropped due to quality concerns at some point in the DLT pipeline. They \nwould like to determine at which table in their pipeline the data is being dropped. Which approach can the data engineer take to identify \nthe table that is dropping the records?",
        "alternativas": [
            "A. They can set up separate expectations for each table when developing their DLT pipeline.",
            "B. They can navigate to the DLT pipeline page, click on the “Error” button, and review the present errors.",
            "C. They can set up DLT to notify them via email when records are dropped.",
            "D. They can navigate to the DLT pipeline page, click on each table, and view the data quality statistics."
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer has a single-task Job that runs each morning before they begin working. After identifying an \nupstream data issue, they need to set up another task to run a new notebook prior to the original task. \nWhich of the following approaches can the data engineer use to set up the new task?",
        "alternativas": [
            "A. They can clone the existing task in the existing Job and update it to run the new notebook.",
            "B. They can create a new task in the existing Job and then add it as a dependency of the original task.",
            "C. They can create a new task in the existing Job and then add the original task as a dependency of the new task.",
            "D. They can create a new job from scratch and add both tasks to run concurrently.",
            "E. They can clone the existing task to a new Job and then edit it to run the new notebook."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer has a single-task Job that runs each morning before they begin working. After identifying an \nupstream data issue, they need to set up another task to run a new notebook prior to the original task. Which approach can the data \nengineer use to set up the new task?",
        "alternativas": [
            "A. They can clone the existing task in the existing Job and update it to run the new notebook.",
            "B. They can create a new task in the existing Job and then add it as a dependency of the original task.",
            "C. They can create a new task in the existing Job and then add the original task as a dependency of the new task.",
            "D. They can create a new job from scratch and add both tasks to run concurrently."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "An engineering manager wants to monitor the performance of a recent project using a Databricks SQL query. For \nthe first week following the projects release, the manager wants the query results to be updated every minute. However, the manager is \nconcerned that the compute resources used for the query will be left running and cost the organization a lot of money beyond the first \nweek of the projects release. \nWhich of the following approaches can the engineering team use to ensure the query does not cost the organization any money beyond \nthe first week of the projects release?",
        "alternativas": [
            "A. They can set a limit to the number of DBUs that are consumed by the SQL Endpoint.",
            "B. They can set the querys refresh schedule to end after a certain number of refreshes.",
            "C. They cannot ensure the query does not cost the organization money beyond the first week of the projects release.",
            "D. They can set a limit to the number of individuals that are able to manage the querys refresh schedule.",
            "E. They can set the querys refresh schedule to end on a certain date in the query scheduler."
        ],
        "resposta": "E"
    },
    {
        "pergunta": "An engineering manager wants to monitor the performance of a recent project using a Databricks SQL query. \nFor the first week following the projects release, the manager wants the query results to be updated every minute. However, the \nmanager is concerned that the compute resources used for the query will be left running and cost the organization a lot of money \nbeyond the first week of the projects release. Which approach can the engineering team use to ensure the query does not cost the \norganization any money beyond the first week of the projects release?",
        "alternativas": [
            "A. They can set a limit to the number of DBUs that are consumed by the SQL Endpoint.",
            "B. They can set the querys refresh schedule to end after a certain number of refreshes.",
            "C. They can set the querys refresh schedule to end on a certain date in the query scheduler.",
            "D. They can set a limit to the number of individuals that are able to manage the querys refresh schedule."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data analysis team has noticed that their Databricks SQL queries are running too slowly when connected to \ntheir always-on SQL endpoint. They claim that this issue is present when many members of the team are running small queries \nsimultaneously. They ask the data engineering team for help. The data engineering team notices that each of the teams queries uses the \nsame SQL endpoint. \nWhich of the following approaches can the data engineering team use to improve the latency of the teams queries?",
        "alternativas": [
            "A. They can increase the cluster size of the SQL endpoint.",
            "B. They can increase the maximum bound of the SQL endpoints scaling range.",
            "C. They can turn on the Auto Stop feature for the SQL endpoint.",
            "D. They can turn on the Serverless feature for the SQL endpoint.",
            "E. They can turn on the Serverless feature for the SQL endpoint and change the Spot Instance Policy to “Reliability Optimized.”"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer wants to schedule their Databricks SQL dashboard to refresh once per day, but they only want \nthe associated SQL endpoint to be running when it is necessary. \nWhich of the following approaches can the data engineer use to minimize the total running time of the SQL endpoint used in the refresh \nschedule of their dashboard?",
        "alternativas": [
            "A. They can ensure the dashboards SQL endpoint matches each of the queries SQL endpoints.",
            "B. They can set up the dashboards SQL endpoint to be serverless.",
            "C. They can turn on the Auto Stop feature for the SQL endpoint.",
            "D. They can reduce the cluster size of the SQL endpoint.",
            "E. They can ensure the dashboards SQL endpoint is not one of the included querys SQL endpoint."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer wants to schedule their Databricks SQL dashboard to refresh once per day, but they only want \nthe associated SQL endpoint to be running when it is necessary. Which approach can the data engineer use to minimize the total running \ntime of the SQL endpoint used in the refresh schedule of their dashboard?",
        "alternativas": [
            "A. They can ensure the dashboards SQL endpoint matches each of the queries SQL endpoints.",
            "B. They can set up the dashboards SQL endpoint to be serverless.",
            "C. They can turn on the Auto Stop feature for the SQL endpoint.",
            "D. They can ensure the dashboards SQL endpoint is not one of the included querys SQL endpoint."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to an \nELT job. The ELT job has its Databricks SQL query that returns the number of input records containing unexpected NULL values. The data \nengineer wants their entire team to be notified via a messaging webhook whenever this value reaches 100. \nWhich of the following approaches can the data engineer use to notify their entire team via a messaging webhook whenever the number \nof NULL values reaches 100?",
        "alternativas": [
            "A. They can set up an Alert with a custom template.",
            "B. They can set up an Alert with a new email alert destination.",
            "C. They can set up an Alert with a new webhook alert destination.",
            "D. They can set up an Alert with one-time notifications.",
            "E. They can set up an Alert without notifications."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to an \nELT job. The ELT job has its Databricks SQL query that returns the number of input records containing unexpected NULL values. The data \nengineer wants their entire team to be notified via a messaging webhook whenever this value reaches 100. Which approach can the data \nengineer use to notify their entire team via a messaging webhook whenever the number of NULL values reaches 100?",
        "alternativas": [
            "A. They can set up an Alert with a custom template.",
            "B. They can set up an Alert with a new email alert destination.",
            "C. They can set up an Alert with a new webhook alert destination.",
            "D. They can set up an Alert with one-time notifications."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A single Job runs two notebooks as two separate tasks. A data engineer has noticed that one of the notebooks is \nrunning slowly in the Jobs current run. The data engineer asks a tech lead for help in identifying why this might be the case. \nWhich of the following approaches can the tech lead use to identify why the notebook is running slowly as part of the Job?",
        "alternativas": [
            "A. They can navigate to the Runs tab in the Jobs UI to immediately review the processing notebook.",
            "B. They can navigate to the Tasks tab in the Jobs UI and click on the active run to review the processing notebook.",
            "C. They can navigate to the Runs tab in the Jobs UI and click on the active run to review the processing notebook.",
            "D. There is no way to determine why a Job task is running slowly.",
            "E. They can navigate to the Tasks tab in the Jobs UI to immediately review the processing notebook."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A single Job runs two notebooks as two separate tasks. A data engineer has noticed that one of the notebooks \nis running slowly in the Jobs current run. The data engineer asks a tech lead for help in identifying why this might be the case. Which \napproach can the tech lead use to identify why the notebook is running slowly as part of the Job?",
        "alternativas": [
            "A. They can navigate to the Runs tab in the Jobs UI to immediately review the processing notebook.",
            "B. They can navigate to the Tasks tab in the Jobs UI and click on the active run to review the processing notebook.",
            "C. They can navigate to the Runs tab in the Jobs UI and click on the active run to review the processing notebook.",
            "D. They can navigate to the Tasks tab in the Jobs UI to immediately review the processing notebook."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer has a Job with multiple tasks that runs nightly. Each of the tasks runs slowly because the clusters \ntake a long time to start. \nWhich of the following actions can the data engineer perform to improve the start up time for the clusters used for the Job?",
        "alternativas": [
            "A. They can use endpoints available in Databricks SQL",
            "B. They can use jobs clusters instead of all-purpose clusters",
            "C. They can configure the clusters to be single-node",
            "D. They can use clusters that are from a cluster pool",
            "E. They can configure the clusters to autoscale for larger data sizes"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A new data engineering team team. has been assigned to an ELT project. The new data engineering team will \nneed full privileges on the database customers to fully manage the project. \nWhich of the following commands can be used to grant full permissions on the database to the new data engineering team?",
        "alternativas": [
            "A. GRANT USAGE ON DATABASE customers TO team;",
            "B. GRANT ALL PRIVILEGES ON DATABASE team TO customers;",
            "C. GRANT SELECT PRIVILEGES ON DATABASE customers TO teams;",
            "D. GRANT SELECT CREATE MODIFY USAGE PRIVILEGES ON DATABASE customers TO team;",
            "E. GRANT ALL PRIVILEGES ON DATABASE customers TO team;"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A new data engineering team has been assigned to work on a project. The team will need access to database \ncustomers in order to see what tables already exist. The team has its own group team. \nWhich of the following commands can be used to grant the necessary permission on the entire database to the new team?",
        "alternativas": [
            "A. GRANT VIEW ON CATALOG customers TO team;",
            "B. GRANT CREATE ON DATABASE customers TO team;",
            "C. GRANT USAGE ON CATALOG team TO customers;",
            "D. GRANT CREATE ON DATABASE team TO customers;",
            "E. GRANT USAGE ON DATABASE customers TO team;"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "Which of the following is a benefit of the Databricks Lakehouse Platform embracing open source technologies? ",
        "alternativas": [
            "A. Cloud-specific integrations ",
            "B. Simplified governance ",
            "C. Ability to scale storage ",
            "D. Ability to scale workloads ",
            "E. Avoiding vendor lock-in "
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer needs to use a Delta table as part of a data pipeline, but they do not know if they have the \nappropriate permissions. In which of the following locations can the data engineer review their permissions on the table?",
        "alternativas": [
            "A. Databricks Filesystem",
            "B. Jobs",
            "C. Dashboards",
            "D. Repos",
            "E. Data Explorer"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer has been given a new record of data: \nid STRING = 'a1' \nrank INTEGER = 6 \nrating FLOAT = 9.4 \nWhich of the following SQL commands can be used to append the new record to an existing Delta table my_table?",
        "alternativas": [
            "A. INSERT INTO my_table VALUES ('a1', 6, 9.4)",
            "B. my_table UNION VALUES ('a1', 6, 9.4)",
            "C. INSERT VALUES ( 'a1' , 6, 9.4) INTO my_table",
            "D. UPDATE my_table VALUES ('a1', 6, 9.4)",
            "E. UPDATE VALUES ('a1', 6, 9.4) my_table"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which of the following describes a scenario in which a data engineer will want to use a single-node cluster? ",
        "alternativas": [
            "A. When they are working interactively with a small amount of data",
            "B. When they are running automated reports to be refreshed as quickly as possible",
            "C. When they are working with SQL within Databricks SQL",
            "D. When they are concerned about the ability to automatically scale with larger data",
            "E. When they are manually running reports with a large amount of data"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer has realized that the data files associated with a Delta table are incredibly small. They want to \ncompact the small files to form larger files to improve performance. Which of the following keywords can be used to compact the small \nfiles?",
        "alternativas": [
            "A. REDUCE",
            "B. OPTIMIZE",
            "C. COMPACTION",
            "D. REPARTITION",
            "E. VACUUM"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "In which of the following file formats is data from Delta Lake tables primarily stored?",
        "alternativas": [
            "A. Delta",
            "B. CSV",
            "C. Parquet",
            "D. JSON",
            "E. A proprietary, optimized format specific to Databricks"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following is stored in the Databricks customers cloud account?",
        "alternativas": [
            "A. Databricks web application",
            "B. Cluster management metadata",
            "C. Repos",
            "D. Data",
            "E. Notebooks"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following can be used to simplify and unify siloed data architectures that are specialized for specific \nuse cases?",
        "alternativas": [
            "A. None of these",
            "B. Data lake",
            "C. Data warehouse",
            "D. All of these",
            "E. Data lakehouse"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data architect has determined that a table with the following format is necessary:\n\n employeeId  | startDate  | avgRating \n------------|-----------|---------- \n a1         | 2009-01-06 | 5.5 \n a2         | 2018-11-21 | 7.1 \n\nWhich of the following code blocks uses SQL DDL commands to create an empty Delta table in this format, regardless of whether a table already exists with this name?",
        "alternativas": [
            "A. CREATE TABLE IF NOT EXISTS table_name ( employeeId STRING, startDate DATE, avgRating FLOAT );",
            "B. CREATE OR REPLACE TABLE table_name AS SELECT employeeId STRING, startDate DATE, avgRating FLOAT USING DELTA;",
            "C. CREATE OR REPLACE TABLE table_name WITH COLUMNS ( employeeId STRING, startDate DATE, avgRating FLOAT ) USING DELTA;",
            "D. CREATE TABLE table_name AS SELECT employeeId STRING, startDate DATE, avgRating FLOAT;",
            "E. CREATE OR REPLACE TABLE table_name ( employeeId STRING, startDate DATE, avgRating FLOAT );"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer has a Python notebook in Databricks, but they need to use SQL to accomplish a specific task \nwithin a cell. They still want all of the other cells to use Python without making any changes to those cells. Which of the following \ndescribes how the data engineer can use SQL within a cell of their Python notebook?",
        "alternativas": [
            "A. It is not possible to use SQL in a Python notebook",
            "B. They can attach the cell to a SQL endpoint rather than a Databricks cluster",
            "C. They can simply write SQL syntax in the cell",
            "D. They can add %sql to the first line of the cell",
            "E. They can change the default language of the notebook to SQL"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following describes a benefit of creating an external table from Parquet rather than CSV when \nusing a CREATE TABLE AS SELECT statement?",
        "alternativas": [
            "A. Parquet files can be partitioned",
            "B. CREATE TABLE AS SELECT statements cannot be used on files",
            "C. Parquet files have a well-defined schema",
            "D. Parquet files have the ability to be optimized",
            "E. Parquet files will become Delta tables"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer wants to create a relational object by pulling data from two tables. The relational object does \nnot need to be used by other data engineers in other sessions. In order to save on storage costs, the data engineer wants to avoid \ncopying and storing physical data. Which of the following relational objects should the data engineer create?",
        "alternativas": [
            "A. Spark SQL Table",
            "B. View",
            "C. Database",
            "D. Temporary view",
            "E. Delta Table"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data analyst has developed a query that runs against Delta table. They want help from the data engineering \nteam to implement a series of tests to ensure the data returned by the query is clean. However, the data engineering team uses Python \nfor its tests rather than SQL. Which of the following operations could the data engineering team use to run the query and operate with \nthe results in PySpark?",
        "alternativas": [
            "A. SELECT * FROM sales",
            "B. spark.delta.table",
            "C. spark.sql",
            "D. There is no way to share data between PySpark and SQL.",
            "E. spark.table"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "Which of the following commands will return the number of null values in the member_id column?",
        "alternativas": [
            "A. SELECT count(member_id) FROM my_table;",
            "B. SELECT count(member_id) - count_null(member_id) FROM my_table;",
            "C. SELECT count_if(member_id IS NULL) FROM my_table;",
            "D. SELECT null(member_id) FROM my_table;",
            "E. SELECT count_null(member_id) FROM my_table;"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer needs to apply custom logic to identify employees with more than 5 years of experience in the array column `employees` in table `stores`. The custom logic should create a new column `exp_employees` that is an array of all employees with more than 5 years of experience for each row. In order to apply this custom logic at scale, the data engineer wants to use the `FILTER` higher-order function. Which of the following code blocks successfully completes this task?",
        "alternativas": [
            "A. SELECT store_id, employees, FILTER(employees, i -> i.years_exp > 5) AS exp_employees FROM stores;",
            "B. SELECT store_id, employees, FILTER(exp_employees, years_exp > 5) AS exp_employees FROM stores;",
            "C. SELECT store_id, employees, FILTER(employees, years_exp > 5) AS exp_employees FROM stores;",
            "D. SELECT store_id, employees, CASE WHEN employees.years_exp > 5 THEN employees ELSE NULL END AS exp_employees FROM stores;",
            "E. SELECT store_id, employees, FILTER(exp_employees, i -> i.years_exp > 5) AS exp_employees FROM stores;"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer has a Python variable table_name that they would like to use in a SQL query. They want to \nconstruct a Python code block that will run the query using table_name. They have the following incomplete code block: \n____(f\"SELECT customer_id, spend FROM {table_name}\"). Which of the following can be used to fill in the blank to successfully complete \nthe task?",
        "alternativas": [
            "A. spark.delta.sql",
            "B. spark.delta.table",
            "C. spark.table",
            "D. dbutils.sql",
            "E. spark.sql"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer has created a new database using the following command: CREATE DATABASE IF NOT EXISTS \ncustomer360; In which of the following locations will the customer360 database be located?",
        "alternativas": [
            "A. dbfs:/user/hive/database/customer360",
            "B. dbfs:/user/hive/warehouse",
            "C. dbfs:/user/hive/customer360",
            "D. More information is needed to determine the correct response",
            "E. dbfs:/user/hive/database"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer that is new to using Python needs to create a Python function to add two integers together and nreturn the sum? Which of the following code blocks can the data engineer use to complete this task?",
        "alternativas": [
            "A. function add_integers(x, y): return x + y",
            "B. function add_integers(x, y): x + y",
            "C. def add_integers(x, y): print(x + y)",
            "D. def add_integers(x, y): return x + y",
            "E. def add_integers(x, y): x + y"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "In which of the following scenarios should a data engineer use the MERGE INTO command instead of the INSERT \nINTO command?",
        "alternativas": [
            "A. When the location of the data needs to be changed",
            "B. When the target table is an external table",
            "C. When the source table can be deleted",
            "D. When the target table cannot contain duplicate records",
            "E. When the source is not a Delta table"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following must be specified when creating a new Delta Live Tables pipeline?",
        "alternativas": [
            "A. A key-value pair configuration",
            "B. The preferred DBU/hour cost",
            "C. A path to cloud storage location for the written data",
            "D. A location of a target database for the written data",
            "E. At least one notebook library to be executed"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer needs to create a table in Databricks using data from a CSV file at location /path/to/csv. They \nrun the following command: \n \n \n \nWhich of the following lines of code fills in the above blank to successfully complete the task?",
        "alternativas": [
            "A. None of these lines of code are needed to successfully complete the task",
            "B. USING CSV",
            "C. FROM CSV",
            "D. USING DELTA",
            "E. FROM \"path/to/csv\""
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then \nperform a streaming write into a new table. The code block used by the data engineer is below: \n \n \n \nIf the data engineer only wants the query to process all of the available data in as many batches as required, which of the following lines \nof code should the data engineer use to fill in the blank?",
        "alternativas": [
            "A. processingTime(1)",
            "B. trigger(availableNow=True)",
            "C. trigger(parallelBatch=True)",
            "D. trigger(processingTime=\"once\")",
            "E. trigger(continuous=\"once\")"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has developed a data pipeline to ingest data from a JSON source using Auto Loader, but the \nengineer has not provided any type inference or schema hints in their pipeline. Upon reviewing the data, the data engineer has noticed \nthat all of the columns in the target table are of the string type despite some of the fields only including float or boolean values. Which of \nthe following describes why Auto Loader inferred all of the columns to be of the string type?",
        "alternativas": [
            "A. There was a type mismatch between the specific schema and the inferred schema",
            "B. JSON data is a text-based format",
            "C. Auto Loader only works with string data",
            "D. All of the fields had at least one null value",
            "E. Auto Loader cannot infer the schema of ingested data"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which of the following data workloads will utilize a Gold table as its source?",
        "alternativas": [
            "A. A job that enriches data by parsing its timestamps into a human-readable format",
            "B. A job that aggregates uncleaned data to create standard summary statistics",
            "C. A job that cleans data by removing malformatted records",
            "D. A job that queries aggregated data designed to feed into a dashboard",
            "E. A job that ingests raw data from a streaming source into the Lakehouse"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following describes the type of workloads that are always compatible with Auto Loader?",
        "alternativas": [
            "A. Streaming workloads",
            "B. Machine learning workloads",
            "C. Serverless workloads",
            "D. Batch workloads",
            "E. Dashboard workloads"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer has joined an existing project and they see the following query in the project repository: \nCREATE STREAMING LIVE TABLE loyal_customers AS \nSELECT customer_id - \nFROM STREAM(LIVE.customers) \nWHERE loyalty_level = 'high'; \nWhich of the following describes why the STREAM function is included in the query? ",
        "alternativas": [
            "A. The STREAM function is not needed and will cause an error.",
            "B. The table being created is a live table.",
            "C. The customers table is a streaming live table.",
            "D. The customers table is a reference to a Structured Streaming query on a PySpark DataFrame.",
            "E. The data in the customers table has been updated since its last run."
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer is using the following code block as part of a batch ingestion pipeline to read from a composable transactions_df = (spark.read.schema(schema).format(\"delta\") .table(\"transactions\") ) ",
        "alternativas": [
            "A. Replace predict with a stream-friendly prediction function",
            "B. Replace schema(schema) with option (\"maxFilesPerTrigger\", 1) ",
            "C. Replace \"transactions\" with the path to the location of the Delta table ",
            "D. Replace format(\"delta\") with format(\"stream\") ",
            "E. Replace spark.read with spark.readStream"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer and data analyst are working together on a data pipeline. The data engineer is working on the \nraw, bronze, and silver layers of the pipeline using Python, and the data analyst is working on the gold layer of the pipeline using SQL. The \nraw source of the pipeline is a streaming input. They now want to migrate their pipeline to use Delta Live Tables. Which of the following \nchanges will need to be made to the pipeline when migrating to Delta Live Tables?",
        "alternativas": [
            "A. None of these changes will need to be made",
            "B. The pipeline will need to stop using the medallion-based multi-hop architecture",
            "C. The pipeline will need to be written entirely in SQL",
            "D. The pipeline will need to use a batch source in place of a streaming source",
            "E. The pipeline will need to be written entirely in Python"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which of the following statements regarding the relationship between Silver tables and Bronze tables is always \ntrue?",
        "alternativas": [
            "A. Silver tables contain a less refined, less clean view of data than Bronze data.",
            "B. Silver tables contain aggregates while Bronze data is unaggregated.",
            "C. Silver tables contain more data than Bronze tables.",
            "D. Silver tables contain a more refined and cleaner view of data than Bronze tables.",
            "E. Silver tables contain less data than Bronze tables."
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineering team has noticed that their Databricks SQL queries are running too slowly when they are \nsubmitted to a non-running SQL endpoint. The data engineering team wants this issue to be resolved. Which of the following approaches \ncan the team use to reduce the time it takes to return results in this scenario?",
        "alternativas": [
            "A. They can turn on the Serverless feature for the SQL endpoint and change the Spot Instance Policy to \"Reliability Optimized.\"",
            "B. They can turn on the Auto Stop feature for the SQL endpoint.",
            "C. They can increase the cluster size of the SQL endpoint.",
            "D. They can turn on the Serverless feature for the SQL endpoint.",
            "E. They can increase the maximum bound of the SQL endpoints scaling range."
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer has a Job that has a complex run schedule, and they want to transfer that schedule to other \nJobs. Rather than manually selecting each value in the scheduling form in Databricks, which of the following tools can the data engineer \nuse to represent and submit the schedule programmatically?",
        "alternativas": [
            "A. pyspark.sql.types.DateType",
            "B. datetime",
            "C. pyspark.sql.types.TimestampType",
            "D. Cron syntax",
            "E. There is no way to represent and submit this information programmatically"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following approaches should be used to send the Databricks Job owner an email in the case that \nthe Job fails?",
        "alternativas": [
            "A. Manually programming in an alert system in each cell of the Notebook",
            "B. Setting up an Alert in the Job page",
            "C. Setting up an Alert in the Notebook",
            "D. There is no way to notify the Job owner in the case of Job failure",
            "E. MLflow Model Registry Webhooks"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "An engineering manager uses a Databricks SQL query to monitor ingestion latency for each data source. The \nmanager checks the results of the query every day, but they are manually rerunning the query each day and waiting for the results. \nWhich of the following approaches can the manager use to ensure the results of the query are updated each day?",
        "alternativas": [
            "A. They can schedule the query to refresh every 1 day from the SQL endpoints page in Databricks SQL.",
            "B. They can schedule the query to refresh every 12 hours from the SQL endpoints page in Databricks SQL.",
            "C. They can schedule the query to refresh every 1 day from the querys page in Databricks SQL.",
            "D. They can schedule the query to run every 1 day from the Jobs UI.",
            "E. They can schedule the query to run every 12 hours from the Jobs UI."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "In which of the following scenarios should a data engineer select a Task in the Depends On field of a new \nDatabricks Job Task?",
        "alternativas": [
            "A. When another task needs to be replaced by the new task",
            "B. When another task needs to fail before the new task begins",
            "C. When another task has the same dependency libraries as the new task",
            "D. When another task needs to use as little compute resources as possible",
            "E. When another task needs to successfully complete before the new task begins"
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to a \ndata analytics dashboard for a retail use case. The job has a Databricks SQL query that returns the number of store-level records where \nsales is equal to zero. The data engineer wants their entire team to be notified via a messaging webhook whenever this value is greater \nthan 0. Which of the following approaches can the data engineer use to notify their entire team via a messaging webhook whenever the \nnumber of stores with $0 in sales is greater than zero?",
        "alternativas": [
            "A. They can set up an Alert with a custom template.",
            "B. They can set up an Alert with a new email alert destination.",
            "C. They can set up an Alert with one-time notifications.",
            "D. They can set up an Alert with a new webhook alert destination.",
            "E. They can set up an Alert without notifications."
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer wants to schedule their Databricks SQL dashboard to refresh every hour, but they only want the \nassociated SQL endpoint to be running when it is necessary. The dashboard has multiple queries on multiple datasets associated with it. \nThe data that feeds the dashboard is automatically processed using a Databricks Job. Which of the following approaches can the data \nengineer use to minimize the total running time of the SQL endpoint used in the refresh schedule of their dashboard?",
        "alternativas": [
            "A. They can turn on the Auto Stop feature for the SQL endpoint.",
            "B. They can ensure the dashboards SQL endpoint is not one of the included querys SQL endpoint.",
            "C. They can reduce the cluster size of the SQL endpoint.",
            "D. They can ensure the dashboards SQL endpoint matches each of the queries SQL endpoints.",
            "E. They can set up the dashboards SQL endpoint to be serverless."
        ],
        "resposta": "E"
    },
    {
        "pergunta": "A data engineer needs access to a table new_table, but they do not have the correct permissions. They can ask \nthe table owner for permission, but they do not know who the table owner is. Which of the following approaches can be used to identify \nthe owner of new_table?",
        "alternativas": [
            "A. Review the Permissions tab in the tables page in Data Explorer",
            "B. All of these options can be used to identify the owner of the table",
            "C. Review the Owner field in the tables page in Data Explorer",
            "D. Review the Owner field in the tables page in the cloud storage solution",
            "E. There is no way to identify the owner of the table"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A new data engineering team team has been assigned to an ELT project. The new data engineering team will \nneed full privileges on the table sales to fully manage the project. Which of the following commands can be used to grant full permissions \non the database to the new data engineering team?",
        "alternativas": [
            "A. GRANT ALL PRIVILEGES ON TABLE sales TO team;",
            "B. GRANT SELECT CREATE MODIFY ON TABLE sales TO team;",
            "C. GRANT SELECT ON TABLE sales TO team;",
            "D. GRANT USAGE ON TABLE sales TO team;",
            "E. GRANT ALL PRIVILEGES ON TABLE team TO sales;"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "Which data lakehouse feature results in improved data quality over a traditional data lake?",
        "alternativas": [
            "A. A data lakehouse stores data in open formats.",
            "B. A data lakehouse allows the use of SQL queries to examine data.",
            "C. A data lakehouse provides storage solutions for structured and unstructured data.",
            "D. A data lakehouse supports ACID-compliant transactions."
        ],
        "resposta": "D"
    },
    {
        "pergunta": "Which of the following data lakehouse features results in improved data quality over a traditional data lake?",
        "alternativas": [
            "A. A data lakehouse provides storage solutions for structured and unstructured data.",
            "B. A data lakehouse supports ACID-compliant transactions. ",
            "C. A data lakehouse allows the use of SQL queries to examine data.",
            "D. A data lakehouse stores data in open formats.",
            "E. A data lakehouse enables machine learning and artificial Intelligence workloads."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "In which scenario will a data team want to utilize cluster pools?",
        "alternativas": [
            "A. An automated report needs to be version-controlled across multiple collaborators.",
            "B. An automated report needs to be runnable by all stakeholders.",
            "C. An automated report needs to be refreshed as quickly as possible.",
            "D. An automated report needs to be made reproducible."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "What is hosted completely in the control plane of the classic Databricks architecture?",
        "alternativas": [
            "A. Worker node",
            "B. Databricks web application",
            "C. Driver node",
            "D. Databricks Filesystem"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer needs to determine whether to use the built-in Databricks Notebooks versioning or version their \nproject using Databricks Repos. What is an advantage of using Databricks Repos over the Databricks Notebooks versioning?",
        "alternativas": [
            "A. Databricks Repos allows users to revert to previous versions of a notebook",
            "B. Databricks Repos is wholly housed within the Databricks Data Intelligence Platform",
            "C. Databricks Repos provides the ability to comment on specific changes",
            "D. Databricks Repos supports the use of multiple branches"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "What is a benefit of the Databricks Lakehouse Architecture embracing open source technologies?",
        "alternativas": [
            "A. Avoiding vendor lock-in",
            "B. Simplified governance",
            "C. Ability to scale workloads",
            "D. Cloud-specific integrations"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer needs to use a Delta table as part of a data pipeline, but they do not know if they have the \nappropriate permissions. In which location can the data engineer review their permissions on the table?",
        "alternativas": [
            "A. Jobs",
            "B. Dashboards",
            "C. Catalog Explorer",
            "D. Repos"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer is running code in a Databricks Repo that is cloned from a central Git repository. A colleague of \nthe data engineer informs them that changes have been made and synced to the central Git repository. The data engineer now needs to \nsync their Databricks Repo to get the changes from the central Git repository.  Which Git operation does the data engineer need to run to \naccomplish this task?",
        "alternativas": [
            "A. Clone",
            "B. Pull",
            "C. Merge",
            "D. Push"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which file format is used for storing Delta Lake Table?",
        "alternativas": [
            "A. CSV",
            "B. Parquet",
            "C. JSON",
            "D. Delta"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has realized that the data files associated with a Delta table are incredibly small. They want to \ncompact the small files to form larger files to improve performance. Which keyword can be used to compact the small files?",
        "alternativas": [
            "A. OPTIMIZE",
            "B. VACUUM",
            "C. COMPACTION",
            "D. REPARTITION"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data architect has determined that a table of the following format is necessary: \n \n \n \nWhich code block is used by SQL DDL command to create an empty Delta table in the above format regardless of whether a table already \nexists with this name?",
        "alternativas": [
            "A. CREATE OR REPLACE TABLE table_name ( employeeId STRING, startDate DATE, avgRating FLOAT )",
            "B. CREATE OR REPLACE TABLE table_name WITH COLUMNS ( employeeId STRING, startDate DATE, avgRating FLOAT ) USING \nDELTA",
            "C. CREATE TABLE IF NOT EXISTS table_name ( employeeId STRING, startDate DATE, avgRating FLOAT )",
            "D. CREATE TABLE table_name AS SELECT employeeId STRING, startDate DATE, avgRating FLOAT"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "What is a benefit of creating an external table from Parquet rather than CSV when using a CREATE TABLE AS \nSELECT statement?",
        "alternativas": [
            "A. Parquet files can be partitioned",
            "B. Parquet files will become Delta tables",
            "C. Parquet files have a well-defined schema",
            "D. Parquet files have the ability to be optimized"
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data engineer runs a statement every day to copy the previous days sales into the table transactions. Each \ndays sales are in their own file in the location \"/transactions/raw\". Today, the data engineer runs the following command to complete \nthis task: \n \n \n \nAfter running the command today, the data engineer notices that the number of records in table transactions has not changed. \nWhat explains why the statement might not have copied any new records into the table?",
        "alternativas": [
            "A. The format of the files to be copied were not included with the FORMAT_OPTIONS keyword.",
            "B. The COPY INTO statement requires the table to be refreshed to view the copied rows.",
            "C. The previous days file has already been copied into the table.",
            "D. The PARQUET file format does not support COPY INTO."
        ],
        "resposta": "C"
    },
    {
        "pergunta": "A data analyst has created a Delta table sales that is used by the entire data analysis team. They want help from \nthe data engineering team to implement a series of tests to ensure the data is clean. However, the data engineering team uses Python \nfor its tests rather than SQL. Which command could the data engineering team use to access sales in PySpark?",
        "alternativas": [
            "A. SELECT * FROM sales",
            "B. spark.table(\"sales\")",
            "C. spark.sql(\"sales\")",
            "D. spark.delta.table(\"sales\")"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has created a new database using the following command: \nCREATE DATABASE IF NOT EXISTS customer360; \nIn which location will the customer360 database be located?",
        "alternativas": [
            "A. dbfs:/user/hive/database/customer360",
            "B. dbfs:/user/hive/warehouse",
            "C. dbfs:/user/hive/customer360",
            "D. dbfs:/user/hive/database"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer needs to create a table in Databricks using data from a CSV file at location `/path/to/csv`.\n\nThey run the following command:\n\nWhich of the following lines of code fills in the above blank to successfully complete the task?",
        "alternativas": [
            "A. FROM \"path/to/csv\"",
            "B. USING CSV",
            "C. FROM CSV",
            "D. USING DELTA"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which SQL keyword can be used to convert a table from a long format to a wide format?",
        "alternativas": [
            "A. TRANSFORM",
            "B. PIVOT",
            "C. SUM",
            "D. CONVERT"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data engineer has a Python variable table_name that they would like to use in a SQL query. They want to construct a Python code block that will run the query using table_name. They have the following incomplete code block: ____(f\\\"SELECT customer_id, spend FROM {table_name}\\\"). What can be used to fill in the blank to successfully complete the task?",
        "alternativas": [
            "A. spark.delta.sql",
            "B. spark.sql",
            "C. spark.table",
            "D. dbutils.sql"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "What is used by Spark to record the offset range of the data being processed in each trigger in order for \nStructured Streaming to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or \nreprocessing?",
        "alternativas": [
            "A. Checkpointing and Write-ahead Logs",
            "B. Replayable Sources and Idempotent Sinks",
            "C. Write-ahead Logs and Idempotent Sinks",
            "D. Checkpointing and Idempotent Sinks"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "What describes when to use the CREATE STREAMING LIVE TABLE (formerly CREATE INCREMENTAL LIVE TABLE) \nsyntax over the CREATE LIVE TABLE syntax when creating Delta Live Tables (DLT) tables using SQL?",
        "alternativas": [
            "A. CREATE STREAMING LIVE TABLE should be used when the subsequent step in the DLT pipeline is static.",
            "B. CREATE STREAMING LIVE TABLE should be used when data needs to be processed incrementally.",
            "C. CREATE STREAMING LIVE TABLE should be used when data needs to be processed through complicated aggregations.",
            "D. CREATE STREAMING LIVE TABLE should be used when the previous step in the DLT pipeline is static."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which type of workloads are compatible with Auto Loader?",
        "alternativas": [
            "A. Streaming workloads",
            "B. Machine learning workloads",
            "C. Serverless workloads",
            "D. Batch workloads"
        ],
        "resposta": "A"
    },
    {
        "pergunta": "A data engineer has developed a data pipeline to ingest data from a JSON source using Auto Loader, but the \nengineer has not provided any type inference or schema hints in their pipeline. Upon reviewing the data, the data engineer has noticed \nthat all of the columns in the target table are of the string type despite some of the fields only including float or boolean values. Why has \nAuto Loader inferred all of the columns to be of the string type?",
        "alternativas": [
            "A. Auto Loader cannot infer the schema of ingested data",
            "B. JSON data is a text-based format",
            "C. Auto Loader only works with string data",
            "D. All of the fields had at least one null value"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which statement regarding the relationship between Silver tables and Bronze tables is always true?",
        "alternativas": [
            "A. Silver tables contain a less refined, less clean view of data than Bronze data.",
            "B. Silver tables contain aggregates while Bronze data is unaggregated.",
            "C. Silver tables contain more data than Bronze tables.",
            "D. Silver tables contain less data than Bronze tables."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "Which query is performing a streaming hop from raw data to a Bronze table?",
        "alternativas": [
            "A. \n(spark.table(\"sales\")\n .groupBy(\"store\")\n .agg(sum(\"sales\"))\n .writeStream\n .option(\"checkpointLocation\", checkpointPath)\n .outputMode(\"complete\")\n .table(\"newSales\"))",
            "B. \n(spark.read.load(rawSalesLocation)\n .write\n .mode(\"append\")\n .table(\"newSales\"))",
            "C. \n(spark.table(\"sales\")\n .withColumn(\"avgPrice\", col(\"sales\") / col(\"units\"))\n .writeStream\n .option(\"checkpointLocation\", checkpointPath)\n .outputMode(\"append\")\n .table(\"newSales\"))",
            "D. \n(spark.readStream.load(rawSalesLocation)\n .writeStream\n .option(\"checkpointLocation\", checkpointPath)\n .outputMode(\"append\")\n .table(\"newSales\"))"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A data engineer has a Job with multiple tasks that runs nightly. Each of the tasks runs slowly because the \nclusters take a long time to start. Which action can the data engineer perform to improve the start up time for the clusters used for the \nJob?",
        "alternativas": [
            "A. They can use endpoints available in Databricks SQL",
            "B. They can use jobs clusters instead of all-purpose clusters",
            "C. They can configure the clusters to autoscale for larger data sizes",
            "D. They can use clusters that are from a cluster pool"
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A data analysis team has noticed that their Databricks SQL queries are running too slowly when connected to \ntheir always-on SQL endpoint. They claim that this issue is present when many members of the team are running small queries \nsimultaneously. They ask the data engineering team for help. The data engineering team notices that each of the teams queries uses the \nsame SQL endpoint. Which approach can the data engineering team use to improve the latency of the teams queries?",
        "alternativas": [
            "A. They can increase the cluster size of the SQL endpoint.",
            "B. They can increase the maximum bound of the SQL endpoints scaling range.",
            "C. They can turn on the Auto Stop feature for the SQL endpoint.",
            "D. They can turn on the Serverless feature for the SQL endpoint."
        ],
        "resposta": "B"
    },
    {
        "pergunta": "A new data engineering team has been assigned to work on a project. The team will need access to database \ncustomers in order to see what tables already exist. The team has its own group team. Which command can be used to grant the \nnecessary permission on the entire database to the new team?",
        "alternativas": [
            "A. GRANT VIEW ON CATALOG customers TO team;",
            "B. GRANT CREATE ON DATABASE customers TO team;",
            "C. GRANT USAGE ON CATALOG team TO customers;",
            "D. GRANT USAGE ON DATABASE customers TO team;"
        ],
        "resposta": "D"
    },
    {
        "pergunta": "A new data engineering team team has been assigned to an ELT project. The new data engineering team will \nneed full privileges on the table sales to fully manage the project. Which command can be used to grant full permissions on the database \nto the new data engineering team?",
        "alternativas": [
            "A. GRANT ALL PRIVILEGES ON TABLE sales TO team;",
            "B. GRANT SELECT CREATE MODIFY ON TABLE sales TO team;",
            "C. GRANT SELECT ON TABLE sales TO team;",
            "D. GRANT ALL PRIVILEGES ON TABLE team TO sales;"
        ],
        "resposta": "A"
    }
]

# ---------------------------------------------------------
# LÓGICA DO SIMULADO
# ---------------------------------------------------------

def restart_quiz():
    # Embaralha todas as perguntas
    all_indices = list(range(len(RAW_DATA)))
    random.shuffle(all_indices)
    
    # Seleciona as primeiras 45 (ou todas se for menor)
    selected_indices = all_indices[:45]
    
    st.session_state.quiz_indices = selected_indices
    st.session_state.current_step = 0
    st.session_state.score = 0
    st.session_state.finished = False
    st.session_state.answer_state = "unanswered" # unanswered, correct, incorrect

if 'quiz_indices' not in st.session_state:
    restart_quiz()

# Se acabou o quiz
if st.session_state.finished:
    st.title("Resultado Final 🏆")
    
    score = st.session_state.score
    total = len(st.session_state.quiz_indices)
    percentage = int((score / total) * 100)
    
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.metric("Pontuação Final", f"{score} / {total}")
        
        if percentage >= 70:
            st.success(f"Parabéns! Você acertou {percentage}% e passou!")
        else:
            st.error(f"Você acertou {percentage}%. É necessário 70% para passar.")
            
        if st.button("Reiniciar Simulado"):
            restart_quiz()
            st.rerun()

else:
    # Lógica da pergunta atual
    step = st.session_state.current_step
    total_q = len(st.session_state.quiz_indices)
    q_index = st.session_state.quiz_indices[step]
    question_data = RAW_DATA[q_index]
    
    # Barra de progresso
    st.progress((step + 1) / total_q)
    st.caption(f"Questão {step + 1} de {total_q}")
    
    # Exibe Pergunta
    st.subheader(question_data["pergunta"])
    
    # Opções
    options = question_data["alternativas"]
    
    # Formulário para evitar reload imediato ao clicar no radio
    with st.form("question_form"):
        choice = st.radio("Escolha uma alternativa:", options, index=None)
        
        # Botão de confirmação
        submitted = st.form_submit_button("Confirmar Resposta", type="primary")
        
        if submitted and choice:
            letter = choice.split(".")[0].strip() # Pega o "A" de "A. Texto..."
            correct_letter = question_data["resposta"].strip().upper()
            
            if letter == correct_letter:
                st.session_state.answer_state = "correct"
                st.session_state.score += 1
                st.success(f"✅ Correto! A resposta é {correct_letter}.")
            else:
                st.session_state.answer_state = "incorrect"
                st.error(f"❌ Errado! A resposta correta era {correct_letter}.")
            
            time.sleep(3.5) # Pequena pausa para ler o feedback
            
            # Avança para próxima
            if step + 1 < total_q:
                st.session_state.current_step += 1
                st.rerun()
            else:
                st.session_state.finished = True
                st.rerun()
        elif submitted and not choice:
            st.warning("Por favor, selecione uma alternativa.")