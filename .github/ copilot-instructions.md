# Introduction

You will be helping me solve a problem at work. I am a data engineer working in azure synapse analytics. I am creating a pipeline to send data from CDZ to DDZ. The DDZ layer is located in azure SQL database.

# Pipeline Overview
The pipeline reads a query located in azure blob storage. the query is written in a `.properties` file format. The query is read inside a notebook and then executed in that notebook using spark SQL. The queries often involve reading a "driver" table and joining it with other tables to create a "target" table/dataaframe that is sent to the DDZ layer in azure SQL database.

The pipeline is designed to be modular and agnostic, meaning it takes different queries from different `.properties` files and different parameters to create different target tables.

There is also a sequence file that contains the order in which the queries should be executed. 

Example of a query properties file:
```properties
df_driver = select * from analytics_sor1_cdz.sor1_driver_table


df_payments = select * from analytics_sor1_cdz.sor_1payments_table


df_target = select \
  a.user_id as UNQ_USER_ID, \
  a.user_name as NAME, \
  b.amount as PAYMENT_AMOUNT, \
  b.category as PAYMENT_CATEGORY, \
  current_date() as LOAD_DATE, \
  'sor1' as SOR_SYS_CD \
  from df_driver a \
  left join df_payments b \
  on a.user_id = b.userid
```

example of a sequence file:
```properties
df_driver
df_payments
df_target
```

all the queries and sequences are stored in the same folder in azure blob storage, subdivided by different folders for different tables.

Once the queries are executed, the resulting dataframe is written to the azure SQL database using the `write.jdbc` method of pyspark.

## Parameters
The parameters that the pipeline takes are:
1. `que_name`: The name of the `.properties` file containing the SQL query
2. `seq_name`: the name of the `.properties` file containing the sequence of queries
3. `table`: the name of the target table
4. `folder`: the folder name in azure blob storage where the `.properties` files are stored
5. `mapping`: (optional) a dictionary that maps column names in the source dataframe to column names in the target table
6. `casts`: (optional) a dictionary that specifies the data types to cast columns in the source dataframe to before writing to the target table
7. `spark_config`: (optional) specific spark optimization configurations for a specific query or table

# Enviroments
DEV - INT - QA - PROD

# Problem Statement
The problem i am facing is that on the PROD environment, the pipeline is failing to write the dataframe to the azure SQL database. Meeting with microsoft support has showed on an internal telemetry that the issue is related to out of memory errors. The issue is not reproducible in the DEV, INT, and QA environments because the data volume is much smaller.

# Copilot working directory
Coptilot will be writing files to the `MDFE issue 2` folder.

# Copilot's role and behavior
After each update from the user, copilot will write a log entry in the `MDFE issue 2` folder. Each new log entry should be in a separate file named `log_entry_<timestamp>.txt` in the `MDFE issue 2` folder. The log entry should include the timestamp, the user's request, and the copilot's response or action taken.


# Copilot limitations
The user cannot provide copilot with files because of privacy concerns from the company the user works at. The user can provide descriptions and code snippets as long as they are not sensitive data or contain passwords.