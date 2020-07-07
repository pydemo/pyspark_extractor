# pyspark_extractor
Local Windows extract from data lake using remote PySpark.


## Usage

 
```
set PYSPARK_EXTRACTOR_LINUX_HOST=home.root.net  
set PYSPARK_EXTRACTOR_LINUX_PWD=***  
python pyspark_extractor.py -q query.sql -o c:\tmp\dump.csv -d 20200604  
```

 
## Params


`query.sql` - input query to run in data lake(Hive/PySpark)
`c:\tmp\dump.csv` - local (Windows OS) query results dump.

## Log



    Elapsed pyspark_extractor.py:connect:---> [ 0.297 ] sec.

    Interactive SSH session established

    [ 0.00 ][ 0.000 ]|||

    [ 0.00 ][ 0.000 ]||| pyspark2

    [ 0.00 ][ 0.004 ]||| $ pyspark2

    [ 0.00 ][ 0.004 ]||| Python 2.7.15 |Anaconda custom (64-bit)| (default, May  1 2018, 23:32:55)

    [ 0.00 ][ 0.004 ]||| [GCC 7.2.0] on linux2

    [ 0.00 ][ 0.004 ]||| Type "help", "copyright", "credits" or "license" for more information.

    [ 2.44 ][ 2.439 ]||| [main] INFO com.unraveldata.agent.ResourceCollector - Unravel Sensor 4.5.0.3rc0008/1.3.11.3 initializing.

    [ 4.49 ][ 2.042 ]||| Setting default log level to "WARN

    [ 4.49 ][ 0.000 ]||| ".

    [ 4.49 ][ 0.000 ]||| To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

    [ 6.00 ][ 1.509 ]||| 20/06/24 11:42:03 WARN util.Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.

    [ 19.53 ][ 13.530 ]||| 20/06/24 11:42:17 WARN util.Json: Json Factory has already been set up. Ignoring this attempt.

    [ 20.24 ][ 0.717 ]||| Welcome to

    [ 20.24 ][ 0.717 ]|||       ____              __

    [ 20.25 ][ 0.721 ]|||      / __/__  ___ _____/ /__

    [ 20.25 ][ 0.721 ]|||     _\ \/ _ \/ _ `/ __/  '_/

    [ 20.25 ][ 0.721 ]|||    /__ / .__/\_,_/_/ /_/\_\   version 2.4.0.cloudera2

    [ 20.25 ][ 0.721 ]|||       /_/

    [ 20.25 ][ 0.721 ]|||

    [ 20.25 ][ 0.721 ]||| Using Python version 2.7.15 (default, May  1 2018 23:32:55)

    [ 20.25 ][ 0.722 ]||| SparkSession available as 'spark'.

    GOT PROMPT: SparkSession available as 'spark'.

    Elapsed pyspark_extractor.py:handshake:---> [ 21.593 ] sec.

    [ 0.0 ][ 0.03 ][ 0.034 ][1]: df=spark.sql("""SELECT * FROM (

    [ 0.0 ][ 0.04 ][ 0.036 ][1]: ... SELECT Counterparty_Account_Type AS Account_Type,

    [ 0.0 ][ 0.04 ][ 0.040 ][1]: ...   Counterparty_Account           AS Account,

    [ 0.0 ][ 0.04 ][ 0.040 ][1]: ...   Branch_Code,

    [ 0.0 ][ 0.04 ][ 0.040 ][1]: ...   Base_Number,
    ...
    [ 75.9 ][ 70.55 ][ 0.200 ][1]: [Stage 9:=====================================================> (196 + 4) / 200]

    [ 76.0 ][ 70.74 ][ 0.186 ][1]: [Stage 9:======================================================>(197 + 3) / 200]

    [ 76.3 ][ 70.95 ][ 0.209 ][1]: [Stage 9:======================================================>(198 + 2) / 200]

    [ 76.5 ][ 71.15 ][ 0.202 ][1]: [Stage 9:======================================================>(199 + 1) / 200]

    [ 77.5 ][ 72.16 ][ 1.008 ][1]: [Stage 10:===============================================>     (178 + 22) / 200]

    [ 77.7 ][ 72.35 ][ 0.193 ][1]: [Stage 10:=====================================================>(199 + 1) / 200]

    [ 79.3 ][ 73.95 ][ 1.601 ][1]: [Stage 11:==================================================>  (190 + 10) / 200]

    [ 79.5 ][ 74.16 ][ 0.206 ][1]: [Stage 11:=====================================================>(197 + 3) / 200]

    [ 79.7 ][ 74.34 ][ 0.182 ][1]: [Stage 11:=====================================================>(198 + 2) / 200]

    [ 80.9 ][ 75.56 ][ 1.212 ][1]: [Stage 11:=====================================================>(199 + 1) / 200]

    [ 81.9 ][ 76.56 ][ 0.787 ][1]: [Stage 18:===================================================>  (191 + 9) / 200]

    [ 82.1 ][ 76.76 ][ 0.200 ][1]: [Stage 18:=====================================================>(198 + 2) / 200]

    [ 82.3 ][ 76.96 ][ 0.200 ][1]: [Stage 18:=====================================================>(199 + 1) / 200]

    [ 84.2 ][ 78.89 ][ 1.348 ][1]: Extracted [10720] to AMC_ALL.csv

    GOT PROMPT

    [ 84.2 ][ 78.90 ][ 0.000 ][2]: print('JobDone')

    EXTRACT: Dumped: -1 records and header

    Elapsed pyspark_extractor.py:dump_data:---> [ 78.899 ] sec.

    Elapsed pyspark_extractor.py:main:---> [ 106.102 ] sec
