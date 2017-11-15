
Spark SQL + DataFrame Demo - NBA Player Stats Analysis
---------------------



This demo uses NBA game data from here:
```
http://www.basketball-reference.com/leagues/NBA_2016_per_game.html
```

```sh
mkdir -p /tmp/demo/data_local/input/spark_sql/nba/player_stats_per_game/
cd /tmp/demo/data_local/input/spark_sql/nba/player_stats_per_game/
```

```sh
hadoop fs -rm -R  /user/aadsyanw/demo/spark_sql/nba/
hadoop fs -mkdir -p  /user/aadsyanw/demo/spark_sql/nba/
hadoop fs -copyFromLocal /tmp/demo/data_local/input/spark_sql/nba/player_stats_per_game  /user/aadsyanw/demo/spark_sql/nba/
```

```sh
pyspark --packages com.databricks:spark-csv_2.10:1.3.0
```



```python
## read in all stats
# stats=sc.textFile('/user/aadsyanw/demo/spark_sql/nba/player_stats_per_game/*.csv')

stats = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/aadsyanw/demo/spark_sql/nba/player_stats_per_game/*.csv')

stats.registerTempTable("stats_tb")

dfPlayers=sqlContext.sql("select age-min_age as exp,stats_tb.* from stats_tb join (select Player,min(age)as min_age from stats_tb group by Player) as t1 on stats_tb.Player=t1.Player order by stats_tb.Player, exp ")
 
dfPlayers.saveAsTable("Players")


dfPlayers.groupBy("age").count().orderBy("age").show()


sqlContext.sql("Select * from Players where Player like '%Stephen Curry%'").show()

sqlContext.sql("Select SUM(`PS/G`) from Players where Player like '%Stephen Curry%'").show()


```
