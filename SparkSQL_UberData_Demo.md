Spark SQL CSV with Python Demo - Uber Dispatch Data Analysis
---------------------

### Overview  
Spark SQL uses a type of Resilient Distributed Dataset called DataFrames.  
DataFrames are composed of Row objects accompanied with a schema which describes  
the data types of each column. A DataFrame may be considered similar to a table  
in a traditional relational database. As you might expect, DataFrames may be  
created from a variety of input sources including CSV text files.

The spark-csv package is described as a “library for parsing and querying CSV data  
with Apache Spark, for Spark SQL and DataFrames”  This library is compatible with 
Spark 1.3 and above.

This demo uses Uber dispatch data from here:
```
https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/Uber-Jan-Feb-FOIL.csv
```

```sh
mkdir -p /tmp/demo/data_local/input/spark_sql/uber/
cd /tmp/demo/data_local/input/spark_sql/uber/
wget https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/Uber-Jan-Feb-FOIL.csv
```


```sh
pyspark --packages com.databricks:spark-csv_2.10:1.3.0
```




```python
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('file:///tmp/demo/data_local/input/spark_sql/uber/Uber-Jan-Feb-FOIL.csv')
```

```python
df.registerTempTable("uber")
```


```python
distinct_bases = sqlContext.sql("select distinct dispatching_base_number from uber")
for b in distinct_bases.collect(): print b
```


```python
df.printSchema()
```

determining which Uber bases is the busiest based on number of trips
```python
sqlContext.sql("""select distinct(`dispatching_base_number`), 
                                sum(`trips`) as cnt from uber group by `dispatching_base_number` 
                                order by cnt desc""").show()

```
 the 5 busiest days based on number of trips in the time range of the data:
 
```python
sqlContext.sql("""select distinct(`date`), 
                                sum(`trips`) as cnt from uber group by `date` 
                                order by cnt desc limit 5""").show()

```
```python

```

