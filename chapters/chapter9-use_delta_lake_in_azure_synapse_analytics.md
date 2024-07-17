[Go back](../README.md)

Essentially, this is about Delta *tables*. 

# Use Delta lake in Azure Synapse Analytics

Linux foundation *Delta Lake* is an open-source storage layer for Spark that enables relational database capabilities for batch and streaming data. 

## Understanding Delta Lake

Benefits of using Delta Lake in Synapse Spark pool: 

* Relational tables that support querying and data modification
    * Allows storing of data in tables that support *CRUD*. Essentially, you can *select*, *insert, *update*, and *delete* rows of data in the same way you would in a relational database system.
* Support for ACID transac's
* Data versioning and *time travel*
    * Allowed by logged transactions in the *transcations log*
* Support for batch and streaming data
    * Native support for streaming data through the Spark Structured Streaming API
    * Delta lake tables can be used as both *sinks* and *sources*
* Standard formats and interoperability
    * Underlying data for Delta Lake tables is stored in Parquet format

## Create Delta Lake tables

Easiest way is to use Spark with the `"delta"` format option. 

```py
# Load a file into a dataframe
df = spark.read.load('/data/mydata.csv', format='csv', header=True)

# Save the dataframe as a delta table
delta_table_path = "/delta/mydata"
df.write.format("delta").save(delta_table_path)
```

After saving the delta table, the path location you specified includes parquet files for the data (regardless of the format of the source file you loaded into the dataframe) and a **_delta_log** folder containing the transaction log for the table.

You can do basic operations such as overwriting, or appending content to it: 

```py
# overwrite
new_df.write.format("delta").mode("overwrite").save(delta_table_path)

# append
new_rows_df.write.format("delta").mode("append").save(delta_table_path)
```


### Conditional updates
Essentially a showcase of the **DeltaTable** API. Instead of making modifications to a dataframe and replacing data, you can use discrete transactional operations:

```py
from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })
```

### Querying a previous version of a table

Delta tables support versioning through the transactional log. It records modifications made to the table, noting the timestamp and version number for each transac; you can use this logged data to *time travel* to a point. 

```py
df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)
```

## Create catalog tables
So far we've considered Delta Lake table instances created from dataframes and modified through the Delta Lake API. You can also define Delta Lake tables as catalog tables in the Hive metastore for your Spark pool, and work with them using SQL.

### *External* vs *managed* tables
* A *managed* table is defined without a specified location, and the data files are stored within the storage used by the metastore. Dropping the table not only removes its metadata from the catalog, but also deletes the folder in which its data files are stored.
* An *external* table is defined for a custom file location, where the data for the table is stored. The metadata for the table is defined in the Spark catalog. Dropping the table deletes the metadata from the catalog, but doesn't affect the data files.

### Creating a catalog table from a dataframe
What we've seen in the previous example. 

```py
# Save a dataframe as a managed table
df.write.format("delta").saveAsTable("MyManagedTable")

## specify a path option to save as an external table
df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")
```

### Creating a catalog table using SQL
You can create a catalog table using the `CREATE TABLE` SQL statement with the `USING DELTA` clause, and an optional `LOCATION` parameter for external table.

```py
spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")
```

or 

```sql
%%sql

CREATE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'
```

### Defining the table schema
So far, the schema has been inferred (or inherited) from the dataframe. In case of external tables, the schema is inherited from the data files.

When creating a new managed table, you define the table schema as with any other SQL table: 

```sql
%%sql

CREATE TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA
```

With delta's, all of the table schemas are enforced - ACID compliance.

### Using the DeltaTableBuilder API
You can also use the DeltaTable API: 

```py
from delta.tables import *

DeltaTable.create(spark) \
  .tableName("default.ManagedProducts") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()
```

## Using catalog tables
Like any other SQL-based relational database table.

```sql
%%sql

SELECT orderid, salestotal
FROM ManagedSalesOrders
```

## Use Delta Lake with streaming data
So far we've explored using static data in files. However, many scenarios involve *streaming* data that must be processed in near real time (for example, readings emitted by IoT devices)

### Spark Structured Streaming
A typical stream processing solution involves constantly reading a stream of data from a *source*, optionally processing it to select specific fields, aggregate and group values, or otherwise manipulate the data, and writing the results to a *sink*.

Spark includes native support for streaming data through *Spark Structured Streaming* API. An SSS dataframe cna read data from many different kinds of streaming sources (notably Kafka)

#### Using a Delta table as a streaming source
Following example reads data from a delta table which stores details of Internet sales orders. It

```py
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Load a streaming dataframe from the Delta Table
stream_df = spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .load("/delta/internetorders")

# Now you can process the streaming data in the dataframe
# for example, show it:
stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

When using delta tables as a streaming source, only *append* operations can be included in the stream. Data modifications will cause an error unless you specify the `ignoreChanges` or `ignoreDeletes` option.

After reading the data from the Delta Lake table into a streaming dataframe, you can use the Spark Structured Streaming API to process it. In the example above, the dataframe is simply displayed; but you could use Spark Structured Streaming to aggregate the data over temporal windows (for example to count the number of orders placed every minute) and send the aggregated results to a downstream process for near-real-time visualization.

#### Using a Delta table as a streaming sink
In the following PySpark example, a stream of data is read from JSON files in a folder. It contains information about status of each IoT device in the format `{"device":"Dev1", "status":"ok"}`. The input stream is a boundless dataframe, which is then written in delta format to a folder location for a Delta Lake table.

```py
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a stream that reads JSON data from a folder
inputPath = '/streamingdata/'
jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
])
stream_df = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

# Write the stream to a delta table
table_path = '/delta/devicetable'
checkpoint_path = '/delta/checkpoint'
delta_stream = stream_df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(table_path)
```

After the streaming process has started, you can query the Delta table to which the streaming output is being written to see the latest data: 

```sql
%%sql

CREATE TABLE DeviceTable
USING DELTA
LOCATION '/delta/devicetable';

SELECT device, status
FROM DeviceTable;
```
To stop the stream of data, use the `stop` method:

```py
delta_stream.stop()
```

## Use Delta Lake in a SQL pool
Synapse also includes a serverless SQL pool runtime that enables running SQL queries against data in a delta tables.

Note that you can only **query** the data from Delta tables in a serverless SQL pool; you can't *update*, *insert* or *delete*.

### Querying Delta tables with OPENROWSET
You can use the OPENROWSET to query data in Delta tables: 

```sql
SELECT *
FROM
    OPENROWSET(
        BULK 'https://mystore.dfs.core.windows.net/files/delta/mytable/',
        FORMAT = 'DELTA'
    ) AS deltadata
```

You could also create a database and add a data source that encapsulates the location of your Delta Lake data files, as shown in this example:

```sql
CREATE DATABASE MyDB
      COLLATE Latin1_General_100_BIN2_UTF8;
GO;

USE MyDB;
GO

CREATE EXTERNAL DATA SOURCE DeltaLakeStore
WITH
(
    LOCATION = 'https://mystore.dfs.core.windows.net/files/delta/'
);
GO

SELECT TOP 10 *
FROM OPENROWSET(
        BULK 'mytable',
        DATA_SOURCE = 'DeltaLakeStore',
        FORMAT = 'DELTA'
    ) as deltadata;
```

## Exercise

### Create delta tables
1. Starts with loading some CSV data into a dataframe: 

```py
%%pyspark
df = spark.read.load('abfss://files@datalakexxxxxxx.dfs.core.windows.net/products/products.csv', format='csv'
## If header exists uncomment line below
, header=True
)
display(df.limit(10))
```

2. Next step is creating a delta table:

```py
delta_table_path = "/delta/products-delta"
df.write.format("delta").save(delta_table_path)
```

3. DeltaTable API update method showcase: 

```py
 from delta.tables import *
 from pyspark.sql.functions import *

 # Create a deltaTable object
 deltaTable = DeltaTable.forPath(spark, delta_table_path)

 # Update the table (reduce price of product 771 by 10%)
 deltaTable.update(
     condition = "ProductID == 771",
     set = { "ListPrice": "ListPrice * 0.9" })

 # View the updated data as a dataframe
 deltaTable.toDF().show(10)
```

4. New dataframe from updated delta table; verify the update

```py
 new_df = spark.read.format("delta").load(delta_table_path)
 new_df.show(10)
```

5. Time travel showcase

```py
 new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
 new_df.show(10)
```

The results show the original version of the delta pre-update. 

6. Show history

```py
 deltaTable.history(10).show(20, False, True)
```

This code example will display the transactions made

```
-RECORD 0------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 version             | 1                                                                                                                                                         
 timestamp           | 2024-07-17 15:57:07                                                                                                                                       
 userId              | null                                                                                                                                                      
 userName            | null                                                                                                                                                      
 operation           | UPDATE                                                                                                                                                    
 operationParameters | {predicate -> (cast(ProductID#323 as int) = 771)}                                                                                                         
 job                 | null                                                                                                                                                      
 notebook            | null                                                                                                                                                      
 clusterId           | null                                                                                                                                                      
 readVersion         | 0                                                                                                                                                         
 isolationLevel      | null                                                                                                                                                      
 isBlindAppend       | false                                                                                                                                                     
 operationMetrics    | {numRemovedFiles -> 1, numCopiedRows -> 294, executionTimeMs -> 8522, scanTimeMs -> 7039, numAddedFiles -> 1, numUpdatedRows -> 1, rewriteTimeMs -> 1483} 
 userMetadata        | null                                                                                                                                                      
-RECORD 1------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 version             | 0                                                                                                                                                         
 timestamp           | 2024-07-17 15:55:52                                                                                                                                       
 userId              | null                                                                                                                                                      
 userName            | null                                                                                                                                                      
 operation           | WRITE                                                                                                                                                     
 operationParameters | {mode -> ErrorIfExists, partitionBy -> []}                                                                                                                
 job                 | null                                                                                                                                                      
 notebook            | null                                                                                                                                                      
 clusterId           | null                                                                                                                                                      
 readVersion         | null                                                                                                                                                      
 isolationLevel      | null                                                                                                                                                      
 isBlindAppend       | true                                                                                                                                                      
 operationMetrics    | {numFiles -> 1, numOutputBytes -> 6046, numOutputRows -> 295}                                                                                             
 userMetadata        | null                                                                                                                                                      


```

### Create catalog tables 
Instead of loading the delta tables into dataframes, in this exercise we're going to work with them by loading them into a catalog table. 

This will encapsulate the data and provide a named table entity that can be referenced in SQL code. 

#### Create an external table

1. Creating a table 

```py
spark.sql("CREATE DATABASE AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)
```

This code creates a new database **AdventureWorks** and an external table named **ProductsExternal**, pointing to the location of parquet files

2. Selecting from the table

```sql
 %%sql

 USE AdventureWorks;

 SELECT * FROM ProductsExternal;
```

#### Create a managed table

1. Creating a table

```py
df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)
```

This creates a managed table named **ProductsManaged** based on the original df (before update). You do specify a path for the parquet files since this is managed by the Hive metastore (shown in the **Location** property)

2. Selecting from the table 


```sql
 %%sql

 USE AdventureWorks;

 SELECT * FROM ProductsManaged;
```

#### Create a table using SQL

1. Creating 

```sql
 %%sql

 USE AdventureWorks;

 CREATE TABLE Products
 USING DELTA
 LOCATION '/delta/products-delta';
```
2. Selecting

Same as others;

### Use delta tables for streaming data
1. Creating the streaming data source

```py
 from notebookutils import mssparkutils
 from pyspark.sql.types import *
 from pyspark.sql.functions import *

 # Create a folder
 inputPath = '/data/'
 mssparkutils.fs.mkdirs(inputPath)

 # Create a stream that reads data from the folder, using a JSON schema
 jsonSchema = StructType([
 StructField("device", StringType(), False),
 StructField("status", StringType(), False)
 ])
 iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

 # Write some event data to the folder
 device_data = '''{"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev2","status":"error"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"error"}
 {"device":"Dev2","status":"ok"}
 {"device":"Dev2","status":"error"}
 {"device":"Dev1","status":"ok"}'''
 mssparkutils.fs.put(inputPath + "data.txt", device_data, True)
 print("Source stream created...")
```
2. Write the streaming service data in delta fromat

```py
 # Write the stream to a delta table
 delta_stream_table_path = '/delta/iotdevicedata'
 checkpointpath = '/delta/checkpoint'
 deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
 print("Streaming to delta sink...")
```

3. Read the streamed data into a dataframe

```py
 # Read the data in delta format into a dataframe
 df = spark.read.format("delta").load(delta_stream_table_path)
 display(df)
```

4. Create a catalog table 

```py
 # create a catalog table based on the streaming sink
 spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))
```

5. Select from the catalog table 

```sql
 %%sql

 SELECT * FROM IotDeviceData;
```

6. Add more data to the streaming source

```sql
 # Add more data to the source stream
 more_data = '''{"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"error"}
 {"device":"Dev2","status":"error"}
 {"device":"Dev1","status":"ok"}'''

 mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)
```

7. Check the new data in the catalog table 

```sql
 %%sql

 SELECT * FROM IotDeviceData;
```

Newly added data from the streaming source is visible. Pog

8. Stop the stream

```py
deltastream.stop()
```

### Query a delta table from a serverless SQL pool
This is just reading from the delta tables using the `OPENROWSET` function:

```sql
 -- This is auto-generated code
 SELECT
     TOP 100 *
 FROM
     OPENROWSET(
         BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/delta/products-delta/',
         FORMAT = 'DELTA'
     ) AS [result]
```

## Knowledge check
1. Which of the following descriptions best fits Delta Lake? 

* A Spark API for exporting data from a relational database into CSV files.
* A relational storage layer for Spark that supports tables based on Parquet files.
* A synchronization solution that replicates data between SQL pools and Spark pools.


<details>
<summary>Answer</summary>
The correct answer is: <b>A relational storage layer for Spark that supports tables based on Parquet files.</b>
</details><br>

2. You've loaded a Spark dataframe with data, that you now want to use in a Delta Lake table. What format should you use to write the dataframe to storage? 

* CSV
* PARQUET
* DELTA

<details>
<summary>Answer</summary>
The correct answer is: <b>DELTA</b>
</details><br>

3. What feature of Delta Lake enables you to retrieve data from previous versions of a table? 

* Spark Structured Streaming
* Time Travel
* Catalog Tables

<details>
<summary>Answer</summary>
The correct answer is: <b>Time Travel</b>
</details><br>

4. You have a managed catalog table that contains Delta Lake data. If you drop the table, what will happen? 

* The table metadata and data files will be deleted.
* The table metadata will be removed from the catalog, but the data files will remain intact.
* The table metadata will remain in the catalog, but the data files will be deleted.

<details>
<summary>Answer</summary>
The correct answer is: <b>The table metadata will be removed from the catalog, but the data files will remain intact.</b>
</details><br>

5. When using Spark Structured Streaming, a Delta Lake table can be which of the following? 
* Only a source
* Only a sink
* Either a source or a sink

<details>
<summary>Answer</summary>
The correct answer is: <b>Either a source or a sink.</b>
</details><br>