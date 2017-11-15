Spark SQL Practice

This example demonstrates how to use sqlContext.sql to create and load a table and select rows from the table into a DataFrame. The next steps use the DataFrame API to filter the rows for salaries greater than 150,000 and show the resulting DataFrame.

1. Transfer file from local to cluster
```sh
cluster: mkdir sample_07

local: scp -P /user/sample_07.txt aadsyanw@montana.com:/home/aadsyanw/sample_07/

```
2. At the command-line, copy the Hue sample_07 data to HDFS:
```sh
hadoop fs -mkdir -p /user/aadsyanw/demo/salary_analysis_spark/

hadoop fs -copyFromLocal /home/aadsyanw/sample_07 /user/aadsyanw/demo/salary_analysis_spark/

hdfs dfs -ls hdfs:///user/aadsyanw/demo/salary_analysis_spark
```

where HUE_HOME defaults to /opt/cloudera/parcels/CDH/lib/hue (parcel installation) or /usr/lib/hue (package installation).

3. Start spark-shell:
```sh
$ spark-shell
```
below 2 lines are not needed in spark shell since they are created for you by spark shell already:
```sh
import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)

```

4. Create a Hive table:
```sh
scala> sqlContext.sql("CREATE TABLE kris_db.sample_07_spark (code string,description string,total_emp int,salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")
```

5. Load data from HDFS into the table:
```sh
scala> sqlContext.sql("LOAD DATA INPATH '/user/aadsyanw/demo/salary_analysis_spark/sample' OVERWRITE INTO TABLE kris_db.sample_07_spark")
```

6. Create a DataFrame containing the contents of the sample_07 table:
```sh
scala> val df = sqlContext.sql("SELECT * from kris_db.sample_07_spark")
```

7. Show all rows with salary greater than 150,000:
```sh
scala> df.filter(df("salary") > 150000).show()
```

The output should be:
```
+-------+--------------------+---------+------+
|   code|         description|total_emp|salary|
+-------+--------------------+---------+------+
|11-1011|    Chief executives|   299160|151370|
|29-1022|Oral and maxillof...|     5040|178440|
|29-1023|       Orthodontists|     5350|185340|
|29-1024|     Prosthodontists|      380|169360|
|29-1061|   Anesthesiologists|    31030|192780|
|29-1062|Family and genera...|   113250|153640|
|29-1063| Internists, general|    46260|167270|
|29-1064|Obstetricians and...|    21340|183600|
|29-1067|            Surgeons|    50260|191410|
|29-1069|Physicians and su...|   237400|155150|
+-------+--------------------+---------+------+
```

