# Sales_CreditCard

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON_OPTS']= "notebook"
# Import Libraries 

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import findspark
spark = SparkSession \
.builder \
.appName("Project_01") \
.config('spark.ui.showConsoleProgress', 'true') \
.config("spark.master", "local[12]") \
.getOrCreate()
df = spark.read.csv('Documents/Bronze/Sales.CreditCard.csv',header=True, inferSchema=True)
df.show(5)
+------------+-------------+--------------+--------+-------+-------------------+
|CreditCardID|     CardType|    CardNumber|ExpMonth|ExpYear|       ModifiedDate|
+------------+-------------+--------------+--------+-------+-------------------+
|           1| SuperiorCard|33332664695310|      11|   2006|2013-07-29 00:00:00|
|           2|  Distinguish|55552127249722|       8|   2005|2013-12-05 00:00:00|
|           3|ColonialVoice|77778344838353|       7|   2005|2014-01-14 00:00:00|
|           4|ColonialVoice|77774915718248|       7|   2006|2013-05-20 00:00:00|
|           5|        Vista|11114404600042|       4|   2005|2013-02-01 00:00:00|
+------------+-------------+--------------+--------+-------+-------------------+
only showing top 5 rows

# Checking Schema

df.printSchema()
root
 |-- CreditCardID: integer (nullable = true)
 |-- CardType: string (nullable = true)
 |-- CardNumber: long (nullable = true)
 |-- ExpMonth: integer (nullable = true)
 |-- ExpYear: integer (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Checking Datas null -- Pandas has limitations #Don't use - Only try

df.toPandas().isna().sum()
CreditCardID    0
CardType        0
CardNumber      0
ExpMonth        0
ExpYear         0
ModifiedDate    0
dtype: int64
# Searching for Nulls

for column in df.columns:
    print(column,df.filter(df[column].isNull()).count())
CreditCardID 0
CardType 0
CardNumber 0
ExpMonth 0
ExpYear 0
ModifiedDate 0
# Show Columns

df.columns
['CreditCardID',
 'CardType',
 'CardNumber',
 'ExpMonth',
 'ExpYear',
 'ModifiedDate']
df.select('CreditCardID CardType CardNumber ExpMonth ExpYear'.split()).show(3)
+------------+-------------+--------------+--------+-------+
|CreditCardID|     CardType|    CardNumber|ExpMonth|ExpYear|
+------------+-------------+--------------+--------+-------+
|           1| SuperiorCard|33332664695310|      11|   2006|
|           2|  Distinguish|55552127249722|       8|   2005|
|           3|ColonialVoice|77778344838353|       7|   2005|
+------------+-------------+--------------+--------+-------+
only showing top 3 rows

#df.select('CreditCardID CardType CardNumber ExpMonth ExpYear').col(('CardType')).filter(col('CardType') == "Vista").distinct().show(100)
# Retrieving only CardType = Vista

df.select('CreditCardID CardType CardNumber ExpMonth ExpYear'.split()).filter(col('CardType') == "Vista").distinct().show(truncate=False)
+------------+--------+--------------+--------+-------+
|CreditCardID|CardType|CardNumber    |ExpMonth|ExpYear|
+------------+--------+--------------+--------+-------+
|1224        |Vista   |11111382209901|7       |2007   |
|1691        |Vista   |11111620961807|11      |2007   |
|2827        |Vista   |11117521163574|3       |2006   |
|3539        |Vista   |11111296552208|9       |2005   |
|5459        |Vista   |11116069076417|2       |2005   |
|6098        |Vista   |11115787175422|6       |2006   |
|8279        |Vista   |11117304875807|2       |2008   |
|8593        |Vista   |11115255653200|11      |2008   |
|9312        |Vista   |11113649287178|2       |2005   |
|9666        |Vista   |11113478834588|8       |2007   |
|10807       |Vista   |11114690173265|5       |2005   |
|11679       |Vista   |11117335726162|6       |2006   |
|13128       |Vista   |11111994467906|1       |2006   |
|14578       |Vista   |11114888317881|10      |2007   |
|15224       |Vista   |11115143788485|11      |2007   |
|15343       |Vista   |11116387729362|10      |2005   |
|16436       |Vista   |11118035210799|8       |2005   |
|17331       |Vista   |11116421861139|2       |2008   |
|18555       |Vista   |11118614544008|9       |2005   |
|809         |Vista   |11119675340022|8       |2006   |
+------------+--------+--------------+--------+-------+
only showing top 20 rows

# Retrieving only CardType = SuperiorCard

df.select('CreditCardID CardType CardNumber ExpMonth ExpYear'.split()).filter(col('CardType') == "SuperiorCard").distinct().show(truncate=False)
+------------+------------+--------------+--------+-------+
|CreditCardID|CardType    |CardNumber    |ExpMonth|ExpYear|
+------------+------------+--------------+--------+-------+
|424         |SuperiorCard|33338284484471|4       |2008   |
|4206        |SuperiorCard|33332435221840|9       |2006   |
|4277        |SuperiorCard|33336841236334|8       |2006   |
|4370        |SuperiorCard|33339552095578|5       |2007   |
|5171        |SuperiorCard|33332739878413|6       |2005   |
|6402        |SuperiorCard|33334181569765|8       |2008   |
|6698        |SuperiorCard|33338295570237|8       |2005   |
|6850        |SuperiorCard|33334723445655|6       |2005   |
|8388        |SuperiorCard|33335829847897|12      |2006   |
|9371        |SuperiorCard|33337801275470|3       |2005   |
|9931        |SuperiorCard|33336073416008|11      |2008   |
|11256       |SuperiorCard|33339582933225|10      |2007   |
|11383       |SuperiorCard|33334829309104|4       |2008   |
|12098       |SuperiorCard|33336528591216|10      |2006   |
|12732       |SuperiorCard|33331019890108|10      |2005   |
|12835       |SuperiorCard|33334530959271|3       |2007   |
|14063       |SuperiorCard|33338025335701|1       |2007   |
|14298       |SuperiorCard|33333898468892|10      |2005   |
|15665       |SuperiorCard|33333129420784|2       |2008   |
|15850       |SuperiorCard|33336712661717|9       |2006   |
+------------+------------+--------------+--------+-------+
only showing top 20 rows

#df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
 #                                .when(df.gender == "F","Female")
  #                               .when(df.gender.isNull() ,"")
   #                              .otherwise(df.gender))
# Changing Numeber by Colum "ExpMonth" for the Name of Month and retrieving only CardType "Vista" and Sorting by CreditCardID
df.withColumn("ExpMonth", when(df.ExpMonth == "1", "January")
             .when(df.ExpMonth == "1", "January")
             .when(df.ExpMonth == "2", "February")
             .when(df.ExpMonth == "3", "March")
             .when(df.ExpMonth == "4", "April")
             .when(df.ExpMonth == "5", "May")
             .when(df.ExpMonth == "6", "June")
             .when(df.ExpMonth == "7", "July")
             .when(df.ExpMonth == "8", "August")
             .when(df.ExpMonth == "9", "September")
             .when(df.ExpMonth == "10", "October")
             .when(df.ExpMonth == "11", "November")
             .when(df.ExpMonth == "12", "December")        
                          ).select('CreditCardID CardType CardNumber ExpMonth ExpYear'.split()).orderBy(col("CreditCardID").asc()).filter(col('CardType') == "Vista").show(3)
+------------+--------+--------------+--------+-------+
|CreditCardID|CardType|    CardNumber|ExpMonth|ExpYear|
+------------+--------+--------------+--------+-------+
|           5|   Vista|11114404600042|   April|   2005|
|          13|   Vista|11119775847802|November|   2005|
|          16|   Vista|11111985451507|  August|   2006|
+------------+--------+--------------+--------+-------+
only showing top 3 rows

df.createOrReplaceTempView("Sales.CreditCard")
spark.catalog.listTables()
[Table(name='creditcard', database='Sales', description=None, tableType='TEMPORARY', isTemporary=True)]
spark.catalog.listDatabases()
[Database(name='default', description='default database', locationUri='file:/C:/Users/data-pyspark/spark-warehouse')]
df = spark.sql("""SELECT
 CreditCardID,
 CardType,
 CardNumber,
 ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE CreditCardID like  '%1%'

""").show(3)
+------------+------------+--------------+--------+-------+
|CreditCardID|    CardType|    CardNumber|ExpMonth|ExpYear|
+------------+------------+--------------+--------+-------+
|           1|SuperiorCard|33332664695310|      11|   2006|
|          10|SuperiorCard|33332126386493|       8|   2008|
|          11|SuperiorCard|33335352517363|      10|   2008|
+------------+------------+--------------+--------+-------+
only showing top 3 rows

df = spark.sql("""SELECT
 CreditCardID,
 CardType,
 CardNumber,
 ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE CreditCardID like  '%1%' and ExpMonth = '4'

""").show(3)
+------------+------------+--------------+--------+-------+
|CreditCardID|    CardType|    CardNumber|ExpMonth|ExpYear|
+------------+------------+--------------+--------+-------+
|          12|SuperiorCard|33334316194519|       4|   2008|
|          61| Distinguish|55553521953614|       4|   2008|
|         105| Distinguish|55558305945748|       4|   2008|
+------------+------------+--------------+--------+-------+
only showing top 3 rows

df = spark.sql("""SELECT
 CreditCardID,
 CardType,
 CardNumber,
 ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE CreditCardID like '%1%' and ExpMonth = '2'

""").show(3)
+------------+-------------+--------------+--------+-------+
|CreditCardID|     CardType|    CardNumber|ExpMonth|ExpYear|
+------------+-------------+--------------+--------+-------+
|         113|        Vista|11117863519103|       2|   2005|
|         165|ColonialVoice|77772090003580|       2|   2007|
|         187|        Vista|11115020407589|       2|   2007|
+------------+-------------+--------------+--------+-------+
only showing top 3 rows

df = spark.sql("""SELECT
 CreditCardID,
 CardType,
 CardNumber,
 ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE ExpMonth like '%2%' and 
 CardType in ("Vista","Distinguish")

""").show(3)
+------------+-----------+--------------+--------+-------+
|CreditCardID|   CardType|    CardNumber|ExpMonth|ExpYear|
+------------+-----------+--------------+--------+-------+
|           9|Distinguish|55553465625901|       2|   2005|
|          54|Distinguish|55553252134216|      12|   2006|
|          58|      Vista|11117610506869|      12|   2008|
+------------+-----------+--------------+--------+-------+
only showing top 3 rows

