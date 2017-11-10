Hive Demo - Salaries Occupation Analysis
---------------------
1. Create tables, load data
2. Query and analyze data
3. Add partitions
```sh
hadoop fs -mkdir -p /user/aadsyanw/demo/salary_analysis/sample_07
hadoop fs -mkdir -p /user/aadsyanw/demo/salary_analysis/sample_08
```

put local data into cluster
```sh
~ Kris$ scp -P [port] /Users/kris/Documents/sample_07.txt aadsyanw@montana.com:/home/aadsyanw/data2/

~ Kris$ scp -P [port] /Users/kris/Documents/sample_08.txt aadsyanw@montana.com:/home/aadsyanw/data2/
```
Copy data from cluster to Hadoop
```sh
hadoop fs -copyFromLocal /home/aadsyanw/data2/sample_07 /user/aadsyanw/demo/salary_analysis/sample_07
hadoop fs -copyFromLocal /home/aadsyanw/data2/sample_08 /user/aadsyanw/demo/salary_analysis/sample_08
```
or
```sh
hadoop fs -put /home/aadsyanw/data2/sample_07 hdfs:///user/aadsyanw/demo/salary_analysis/sample_07
hadoop fs -put /home/aadsyanw/data2/sample_08 hdfs:///user/aadsyanw/demo/salary_analysis/sample_08

```
choose database in hive;
```sh
hive；
USE kris_db;
```

### Step 1 - Create tables
split column by space ('\t')
```sql
CREATE EXTERNAL TABLE sample_07(
code string,
description string,
total_emp int,
salary int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/user/aadsyanw/demo/salary_analysis/sample_07';
```

```sql
CREATE EXTERNAL TABLE sample_08(
code string,
description string,
total_emp int,
salary int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/user/aadsyanw/demo/salary_analysis/sample_08';
```

### Step 2 - analyze the data 
find one single record
```sql
SELECT * FROM sample_07 WHERE code = '11-1031';
```

Visualize results - list salary distribution
```sql
SELECT
description,
salary
FROM sample_07 
WHERE salary IS NOT NULL
ORDER BY salary DESC LIMIT 20;
```

Find out which makes more money in sample_08 than in sample_07, by difference percentage
```sql
SELECT
sample_08.description,
sample_08.salary,
sample_07.salary,
100* (sample_08.salary - sample_07.salary)/sample_07.salary as percentage
FROM sample_08
JOIN sample_07 ON sample_08.code = sample_07.code
where sample_08.salary > sample_07.salary
ORDER BY percentage DESC
LIMIT 20;
```


Find out the average total employee number and average salaries
```sql
SELECT
a.code,
a.description,
(a.total_emp + b.total_emp) /2 AS avg_total_emp,
(a.salary + b.salary) /2 AS avg_salary
FROM sample_07 a
JOIN sample_08 b ON a.code = b.code
WHERE a.salary IS NOT NULL AND b.salary IS NOT NULL
ORDER BY avg_salary DESC
LIMIT 20;
```


### Step 3 - add two files as partitions of a larger table

```sql
CREATE EXTERNAL TABLE sample_all(
code string,
description string,
total_emp int,
salary int)
partitioned by (year string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/user/randy/demo/salary_analysis/';
```

```sql
ALTER TABLE sample_all ADD IF NOT EXISTS PARTITION (year='07') LOCATION '/user/randy/demo/salary_analysis/sample_07';
ALTER TABLE sample_all ADD IF NOT EXISTS PARTITION (year='08') LOCATION '/user/randy/demo/salary_analysis/sample_08';
```

```sql
DESC FORMATTED sample_all;

SHOW PARTITIONS sample_all;
```

