[Go back](../README.md)

This is going to be a long and text-heavy one. However, it is very important to have a clear understanding of these concepts, as they are the backbone of the entire industry. 

# Analyze data in a relational data warehouse

Relational data warehouses are at the center of most enterprise BI solutions. Common pattern is denormalized, multidimensional schema (star schema).

In this lesson we will use dedicated SQL pools in Synapse to host the databases capable of hosting and querying huge volumes of data in relational tables. 

## Design a data warehouse schema

As in all relational databases, a data warehouse contains tables in which the data you want to analyze is stored. 

These tables are organized in a schema that is optimized for *multidimensional* modeling, in which numerical measures associated with events known as **facts** can be aggregated by attributes of associated entities across multiple **dimensions**.

For example, sales order will be your fact table (it includes measures associated with it, such as the amount paid or the quantity of items), and it will be aggregated by attributes of the date on which the sale occurred, the customer, the store, and so on. (these are your dimensions)

### Dimension tables

Dimension tables describe business entities, such as products, people, places and dates. They contain columns for attributes of an entity; for example for customer - first name, last name, an email address. 

It is common for dims to contain **two** key columns: 

* a *surrogate* key that is specific to the data warehouse and uniquely identifies each row in the dim - usually an incrementing integer
* an *alternate* key, often a *natural* or a *business* key that is used to identify a specific instance of an entity (Montepio used HASH of all columns - *hash_chave*)

It is also important to understand WHY an alternate key is used. Attributes of entities may change over time - for example, customer may change their address. Since data warehouse is used to support historic reporting, you may want to retain records of old AND new address - **slowly changing dimensions**

| CustomerKey | CustomerAltKey | Name          | Email              | Street          | City      | PostalCode | CountryRegion |
|-------------|----------------|---------------|--------------------|-----------------|-----------|------------|---------------|
| 123         | I-543          | Navin Jones   | navin1@contoso.com | 1 Main St.      | Seattle   | 90000      | United States |
| 124         | R-589          | Mary Smith    | mary2@contoso.com  | 234 190th Ave   | Buffalo   | 50001      | United States |
| 125         | I-321          | Antoine Dubois| antoine1@contoso.com | 2 Rue Jolie     | Paris     | 20098      | France        |
| 126         | I-543          | Navin Jones   | navin1@contoso.com | 24 125th Ave.   | New York  | 50000      | United States |
| ...         | ...            | ...           | ...                | ...             | ...       | ...        | ...           |


Notice Navin Jones having two records, but the **CustomerKey** columns is different. Usually these tables also include columns for identifying what is the *active* record. For example:
* startDate
* endDate
* isActive

In addition to dims that represent business entities, it's common for a data warehouse to include a dimension table that represents *time*. Depending on the type of data you need to analyze, the lowest granularity (referred to as the *grain*) of a time dimension could represent times (to the hour, second, millisecond, nanosecond, or even lower), or dates.

| DateKey   | DateAltKey  | DayOfWeek | DayOfMonth | Weekday | Month | MonthName | Quarter | Year |
|-----------|-------------|-----------|------------|---------|-------|-----------|---------|------|
| 19990101  | 01-01-1999  | 6         | 1          | Friday  | 1     | January   | 1       | 1999 |
| ...       | ...         | ...       | ...        | ...     | ...   | ...       | ...     | ...  |
| 20220101  | 01-01-2022  | 7         | 1          | Saturday| 1     | January   | 1       | 2022 |
| 20220102  | 02-01-2022  | 1         | 2          | Sunday  | 1     | January   | 1       | 2022 |
| ...       | ...         | ...       | ...        | ...     | ...   | ...       | ...     | ...  |
| 20301231  | 31-12-2030  | 3         | 31         | Tuesday | 12    | December  | 4       | 2030 |

### Fact tables

Fact tables store details of observations or events; for example, sales orders, stock balances, recorded temperatures etc. A fact table contains columns for numeric values that can be aggregated by dimensions (connected via key columns)

| OrderDateKey | CustomerKey | StoreKey | ProductKey | OrderNo | LineItemNo | Quantity | UnitPrice | Tax  | ItemTotal |
|--------------|-------------|----------|------------|---------|------------|----------|-----------|------|-----------|
| 20220101     | 123         | 5        | 701        | 1001    | 1          | 2        | 2.50      | 0.50 | 5.50      |
| 20220101     | 123         | 5        | 765        | 1001    | 2          | 1        | 2.00      | 0.20 | 2.20      |
| 20220102     | 125         | 2        | 723        | 1002    | 1          | 1        | 4.99      | 0.49 | 5.48      |
| 20220103     | 126         | 1        | 823        | 1003    | 1          | 1        | 7.99      | 0.80 | 8.79      |
| ...          | ...         | ...      | ...        | ...     | ...        | ...      | ...       | ...  | ...       |

A fact table's dimension key columns determine its grain. For example, the sales orders fact table includes keys for dates, customers, stores, and products. An order might include multiple products, so the grain represents line items for individual products sold in stores to customers on specific days.

## Data warehouse schema designs

In operational databases, the data is *normalized* to reduce duplication, however in data warehouses the data is *de-normalized* to reduce the number of joins required to query the data. 

Often, a data warehouse is organized as a *star* schema, in which a fact table is directly related to the dimension tables:

![alt text](../img/star-schema.png)

The attributes of an entity can be used to aggregate measures in fact tables over multiple hierarchical levels - for example, to find total sales revenue by country or region, city, postal code, or individual customer. 

However, when an entity has a large number of hierarchical attribute levels, or when some attributes can be shared by multiple dimensions (for example, both customers and stores have a geographical address), it can make sense to apply some normalization to the dimension tables and create a snowflake schema, as shown in the following example:


![alt text](../img/snowflake-schema.png)

## Create data warehouse tables

### Creating a dedicated SQL pool
Won't go over too much detail here, as I think this is more of infra guys job than DE.

But essentially, in the existing Synapse workspace navigate to the **Manage** page and use the dedicated SQL pool creation wizard

### Considerations for creating tables

To create tables in the dedicated SQL pool, you use the `CREATE TABLE` or `CREATE EXTERNAL TABLE` commands. Option used depends on whether you're creating: 

* Fact tables
* Dimension tables
* Staging tables

*Staging* tables are often used as part of the data warehousing loading process to ingest data from source systems. 

#### Data integrity constraints

Since dedicated SQL pools don't support *foreign key* and *unique* constraints as found in RDBMS's, the job of maintaining uniqueness and referential integrity for keys falls upon the job that is used to load the data. 

#### Indexes

The default index type in dedicated SQL pools is *clustered columnstore*. Some tables may include data types that can't be included in a clustered columnstore (such as VARBINARY(MAX)), in which case a clustered index can be used instead.

#### Distribution

Dedicated SQL pools use [massively parallel processing (MPP architecture)](https://learn.microsoft.com/en-us/azure/architecture/data-guide/relational-data/data-warehousing#data-warehousing-in-azure), in which the data in table is distributed for processing across pool of nodes.

Synapse supports the following kinds of distribution: 

* **Hash**: A deterministic hash value is calculated for the specified column and used to assign the row to a compute node.
* **Round-Robin**: Rows are distributed evenly across all compute nodes
* **Replicated**: A copy of the table is stored on each compute node.

Recommended options for distributing the table: 

| Table type  | Recommended distribution option                                                                                                                                  |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Dimension   | Use replicated distribution for smaller tables to avoid data shuffling when joining to distributed fact tables. If tables are too large to store on each compute node, use hash distribution. |
| Fact        | Use hash distribution with clustered columnstore index to distribute fact tables across compute nodes.                                                           |
| Staging     | Use round-robin distribution for staging tables to evenly distribute data across compute nodes.                                                                  |
### Creating dimension tables

Just ensure that the table definition includes surrogate and alternate keys alongside the attributes of the dimension that you want to use. 

The following example shows a `CREATE TABLE` statement for a hypothetical **DimCustomer** dimension table.

```sql
CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

If you intend to use a *snowflake* schema in which dimension tables are related to one another, you should include the key for the *parent* dimension in the definition of the *child* dimension table. For example, the following SQL code could be used to move the geographical address details from the **DimCustomer** table to a separate **DimGeography** dimension table:

```sql
CREATE TABLE dbo.DimGeography
(
    GeographyKey INT IDENTITY NOT NULL,
    GeographyAlternateKey NVARCHAR(10) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    GeographyKey INT NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

#### Time dimension tables
Enable aggregating data by multiple hierarchical levels of time interval.

```sql
CREATE TABLE dbo.DimDate
( 
    DateKey INT NOT NULL,
    DateAltKey DATETIME NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(15) NOT NULL,
    MonthOfYear INT NOT NULL,
    MonthName NVARCHAR(15) NOT NULL,
    CalendarQuarter INT  NOT NULL,
    CalendarYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    FiscalYear INT NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

A common pattern when creating a dimension table for dates is to use the numeric date in *DDMMYYYY* or *YYYYMMDD* format as an integer surrogate key, and the date as a `DATE` or `DATETIME` datatype as the alternate key.


### Creating fact tables
Fact tables include the keys for each dimension to which they're related, and the attributes and numeric measures for specific events or observations that you want to analyze.

```sql
CREATE TABLE dbo.FactSales
(
    OrderDateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    OrderNumber NVARCHAR(10) NOT NULL,
    OrderLineItem INT NOT NULL,
    OrderQuantity SMALLINT NOT NULL,
    UnitPrice DECIMAL NOT NULL,
    Discount DECIMAL NOT NULL,
    Tax DECIMAL NOT NULL,
    SalesAmount DECIMAL NOT NULL
)
WITH
(
    DISTRIBUTION = HASH(OrderNumber),
    CLUSTERED COLUMNSTORE INDEX
);
```

### Creating staging tables
Temporary storage for data as it's being loaded into a data warehouse. 

Typical pattern is to structure the table to make it as efficient as possible to ingest the data from its external source (often files in data lake) into relational databases, and then use SQL to load it from staging tables into dims and facts

```sql
CREATE TABLE dbo.StageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
```

#### Using external tables
In some cases, if the data to be loaded is in files with an appropriate structure, it can be more effective to create external tables that reference the file location. This way, the data can be read directly from the source files instead of being loaded into the relational store. 

```sql
-- External data source links to data lake location
CREATE EXTERNAL DATA SOURCE StagedFiles
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/stagedfiles/'
);
GO

-- External format specifies file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- External table references files in external data source
CREATE EXTERNAL TABLE dbo.ExternalStageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DATA_SOURCE = StagedFiles,
    LOCATION = 'products/*.parquet',
    FILE_FORMAT = ParquetFormat
);
GO
```

## Load data warehouse tables

At a basic level, loading a data warehouse is typically achieved by adding new data from files in a data lake into tables in the data warehouse:

```sql
COPY INTO dbo.StageProducts
    (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
FROM 'https://mydatalake.blob.core.windows.net/data/stagedfiles/products/*.parquet'
WITH
(
    FILE_TYPE = 'PARQUET',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);
```

### Considerations for designing a data warehouse load process

Common pattern for loading a data warehouse is transfering data from source systems to files in a data lake, ingest the file data into staging tables, and then finally use SQL statements to copy from staging tables into dims and facts. 

Usually data loading is performed as a periodic batch process in which inserts and updates to the data warehouse are coordinated to occur at a regular interval (for example, daily, weekly, or monthly).

How a data warehouse load process typically looks:

1. Ingest the new data to be loaded into a data lake, applying pre-load cleansing or transformations as required.
2. Load the data from files into staging tables in the relational data warehouse.
3. Load the dimension tables from the dimension data in the staging tables, updating existing rows or inserting new rows and generating surrogate key values as necessary.
4. Load the fact tables from the fact data in the staging tables, looking up the appropriate surrogate keys for related dimensions.
5. Perform post-load optimization by updating indexes and table distribution statistics.

## Query a data warehouse
Once the tables have been loaded, you can use SQL to query the tables and analyze the data contained within.

### Aggregating measures by dimension attributes
Most data analytics with a data warehouse involves aggregating numeric measures in fact tables by attributes in dimension tables. Because of the way a star or snowflake schema is implemented, queries to perform this kind of aggregation rely on `JOIN` clauses to connect fact tables to dimension tables.

```sql
SELECT  dates.CalendarYear,
        dates.CalendarQuarter,
        SUM(sales.SalesAmount) AS TotalSales
FROM dbo.FactSales AS sales
JOIN dbo.DimDate AS dates ON sales.OrderDateKey = dates.DateKey
GROUP BY dates.CalendarYear, dates.CalendarQuarter
ORDER BY dates.CalendarYear, dates.CalendarQuarter;
```

Result:

| CalendarYear | CalendarQuarter | TotalSales |
|--------------|------------------|------------|
| 2020         | 1                | 25980.16   |
| 2020         | 2                | 27453.87   |
| 2020         | 3                | 28527.15   |
| 2020         | 4                | 31083.45   |
| 2021         | 1                | 34562.96   |
| 2021         | 2                | 36162.27   |
| ...          | ...              | ...        |


For example, the following code extends the previous example to break down the quarterly sales totals by city based on the customer's address details in the DimCustomer table:

```sql
SELECT  dates.CalendarYear,
        dates.CalendarQuarter,
        custs.City,
        SUM(sales.SalesAmount) AS TotalSales
FROM dbo.FactSales AS sales
JOIN dbo.DimDate AS dates ON sales.OrderDateKey = dates.DateKey
JOIN dbo.DimCustomer AS custs ON sales.CustomerKey = custs.CustomerKey
GROUP BY dates.CalendarYear, dates.CalendarQuarter, custs.City
ORDER BY dates.CalendarYear, dates.CalendarQuarter, custs.City;
```

#### Joins in a snowflake schema
When using a snowflake schema, dimensions may be partially normalized; requiring multiple joins to relate fact tables to snowflake dimensions.
For example, suppose your data warehouse includes a **DimProduct** dimension table from which the product categories have been normalized into a separate **DimCategory** table. A query to aggregate items sold by product category might look similar to the following example:

```sql
SELECT  cat.ProductCategory,
        SUM(sales.OrderQuantity) AS ItemsSold
FROM dbo.FactSales AS sales
JOIN dbo.DimProduct AS prod ON sales.ProductKey = prod.ProductKey
JOIN dbo.DimCategory AS cat ON prod.CategoryKey = cat.CategoryKey
GROUP BY cat.ProductCategory
ORDER BY cat.ProductCategory;
```

Result:

| ProductCategory | ItemsSold |
|-----------------|-----------|
| Accessories     | 28271     |
| Bits and pieces | 5368      |
| ...             | ...       |

### Using ranking functions
Another common kind of analytical query is to partition the results based on a dimension attribute and rank the results within each partition. For example, you might want to rank stores each year by their sales revenue. 

T-SQL Ranking functions: 

* **ROW_NUMBER** returns the ordinal position of the row within the partition. For example, the first row is numbered 1, the second 2, and so on.
* **RANK** returns the ranked position of each row in the ordered results. For example, in a partition of stores ordered by sales volume, the store with the highest sales volume is ranked 1. If multiple stores have the same sales volumes, they'll be ranked the same, and the rank assigned to subsequent stores reflects the number of stores that have higher sales volumes - including ties.
* **DENSE_RANK** ranks rows in a partition the same way as RANK, but when multiple rows have the same rank, subsequent rows are ranking positions ignore ties.
* **NTILE** returns the specified percentile in which the row falls. For example, in a partition of stores ordered by sales volume, NTILE(4) returns the quartile in which a store's sales volume places it.

Example:

```sql
SELECT  ProductCategory,
        ProductName,
        ListPrice,
        ROW_NUMBER() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS RowNumber,
        RANK() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS Rank,
        DENSE_RANK() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS DenseRank,
        NTILE(4) OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS Quartile
FROM dbo.DimProduct
ORDER BY ProductCategory;
```

This query partitions products into groupings based on the category, and the relative position of each product is determined based on its list price, within each category partition. 

| ProductCategory  | ProductName     | ListPrice | RowNumber | Rank | DenseRank | Quartile |
|------------------|-----------------|-----------|-----------|------|-----------|----------|
| Accessories      | Widget          | 8.99      | 1         | 1    | 1         | 1        |
| Accessories      | Knicknak        | 8.49      | 2         | 2    | 2         | 1        |
| Accessories      | Sprocket        | 5.99      | 3         | 3    | 3         | 2        |
| Accessories      | Doodah          | 5.99      | 4         | 3    | 3         | 2        |
| Accessories      | Spangle         | 2.99      | 5         | 5    | 4         | 3        |
| Accessories      | Badabing        | 0.25      | 6         | 6    | 5         | 4        |
| Bits and pieces  | Flimflam        | 7.49      | 1         | 1    | 1         | 1        |
| Bits and pieces  | Snickity wotsit | 6.99      | 2         | 2    | 2         | 1        |
| Bits and pieces  | Flange          | 4.25      | 3         | 3    | 3         | 2        |
| ...              | ...             | ...       | ...       | ...  | ...       | ...      |

### Retrieving an approximate count
While the purpose of a data warehouse is primarily to support analytical data models and reports for the enterprise; data analysts and data scientists often need to perform some initial data exploration, just to determine the basic scale and distribution of the data.

For example, the following query uses the COUNT function to retrieve the number of sales for each year in a hypothetical data warehouse:

```sql
SELECT dates.CalendarYear AS CalendarYear,
    COUNT(DISTINCT sales.OrderNumber) AS Orders
FROM FactSales AS sales
JOIN DimDate AS dates ON sales.OrderDateKey = dates.DateKey
GROUP BY dates.CalendarYear
ORDER BY CalendarYear;
```

Because of the volume of data, using count can take a significant time. In such cases, you can use `APPROX_COUNT_DISTINCT`

```sql
SELECT dates.CalendarYear AS CalendarYear,
    APPROX_COUNT_DISTINCT(sales.OrderNumber) AS ApproxOrders
FROM FactSales AS sales
JOIN DimDate AS dates ON sales.OrderDateKey = dates.DateKey
GROUP BY dates.CalendarYear
ORDER BY CalendarYear;
```

The result is guaranteed to have a maximum error rate of 2% with 97% probability.

## Exercise
TODO!