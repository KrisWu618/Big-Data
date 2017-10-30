Hive Demo
---------------------
1. Create tables, load data
2. Query and analyze data
3. Add partitions

### Step 1. create a text file in local system
```sh
mkdir -p /home/aadsyanw/demo/students_tb/students/
vi /home/aadsyanw/demo/students_tb/students/students.txt
```

### Step 2. paste below rows into the file
```sh
1,Nic,Raboy,Merced,California
2,Jane,Doe,Newark,New York
3,John,Lee,Las Vegas,Nevada
4,Maria,Campos,Modesto,California
```
### press Esc to return to command mode.  Press Shift-z-z to save file. https://kb.iu.edu/d/adxz
### now we have data without column names.

### Step 3. Create an empty table for students
```sql
hive；
CREATE DATABASE kris_db;
use kris_db;
CREATE TABLE students(id INT, first_name STRING, last_name STRING, city STRING, state STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
```

### Step 4. Load the text file into this table

```sql
LOAD DATA LOCAL INPATH '/home/aadsyanw/demo/students_tb/students/students.txt' OVERWRITE INTO TABLE students;
```

### Step 5. query this table to validate it
```sql
SELECT * FROM students;
```

