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

df = spark.sql("""SELECT distinct
 CreditCardID,
 CardType,
 CardNumber,
 ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE ExpMonth like '%1%' and 
 CardType in ("Vista","Distinguish","ColonialVoice") and 
 ExpYear = "2005"
 

""").show(3)
+------------+-----------+--------------+--------+-------+
|CreditCardID|   CardType|    CardNumber|ExpMonth|ExpYear|
+------------+-----------+--------------+--------+-------+
|         838|      Vista|11112072966443|      11|   2005|
|        1712|Distinguish|55555294450605|      12|   2005|
|        2541|Distinguish|55555171836955|      10|   2005|
+------------+-----------+--------------+--------+-------+
only showing top 3 rows

df = spark.sql("""SELECT distinct
 CreditCardID,
 CardType,
 CardNumber,
 --ExpMonth,
 
 CASE WHEN ExpMonth = 1 THEN "January"
            WHEN ExpMonth = 2 THEN "February"
            WHEN ExpMonth = 3 THEN "March"
            WHEN ExpMonth = 4 THEN "April"
            WHEN ExpMonth = 5 THEN "May"
            WHEN ExpMonth = 6 THEN "June"
            WHEN ExpMonth = 7 THEN "July"
            WHEN ExpMonth = 8 THEN "August"
            WHEN ExpMonth = 9 THEN "September"
            WHEN ExpMonth = 10 THEN "October"
            WHEN ExpMonth = 11 THEN "November"
            WHEN ExpMonth = 12 THEN "December"
            ELSE '' END AS ExpMonth,
 ExpYear
 
 FROM creditcard
 
 WHERE --ExpMonth like '%1%' and 
 CardType in ("Vista","Distinguish","ColonialVoice") --and 
 --ExpYear = "2005"
 --Order by ExpMonth asc
 Order by ExpYear asc
 
""").show(50)
+------------+-------------+--------------+---------+-------+
|CreditCardID|     CardType|    CardNumber| ExpMonth|ExpYear|
+------------+-------------+--------------+---------+-------+
|       14020|  Distinguish|55552475527322|  October|   2005|
|       10584|        Vista|11115277409906|  January|   2005|
|       15773|  Distinguish|55553254355173|     July|   2005|
|       12711|  Distinguish|55555847625128| November|   2005|
|       16180|  Distinguish|55551934660898| December|   2005|
|        1448|  Distinguish|55552641791214|  January|   2005|
|       16605|        Vista|11112208536481|    March|   2005|
|        1891|  Distinguish|55557422251046|   August|   2005|
|       17310|        Vista|11112622842687|      May|   2005|
|        3648|        Vista|11115603520031|     June|   2005|
|       18021|ColonialVoice|77773296332654| February|   2005|
|        4881|ColonialVoice|77771741451738| November|   2005|
|       18726|  Distinguish|55555323305703|     July|   2005|
|        5094|        Vista|11119689632759|  January|   2005|
|       19191|        Vista|11118737765206|  January|   2005|
|        6661|ColonialVoice|77772838744146| November|   2005|
|        2866|ColonialVoice|77778769829144| February|   2005|
|        7719|  Distinguish|55559423624012|  January|   2005|
|        3565|ColonialVoice|77778492517106|  October|   2005|
|        9690|  Distinguish|55553418861306|  January|   2005|
|        5214|  Distinguish|55556181195570|    March|   2005|
|       10451|        Vista|11113246138766|    March|   2005|
|        5925|        Vista|11117569077579|    March|   2005|
|         687|  Distinguish|55555073412599|     July|   2005|
|        6256|ColonialVoice|77774806034407|September|   2005|
|       13875|        Vista|11116667983089|    March|   2005|
|        8208|  Distinguish|55555190162911|   August|   2005|
|       15415|        Vista|11116283345747|     June|   2005|
|        8231|  Distinguish|55552235386334|    March|   2005|
|       16837|ColonialVoice|77777277597778|     June|   2005|
|       10024|ColonialVoice|77776147702243|      May|   2005|
|       17758|ColonialVoice|77779827585390|  October|   2005|
|       10310|  Distinguish|55551019954238|    April|   2005|
|       19081|ColonialVoice|77775759317859|     June|   2005|
|       11040|        Vista|11118999547346| December|   2005|
|        1389|        Vista|11114085487755|      May|   2005|
|       13067|        Vista|11114788190010|     July|   2005|
|        3809|ColonialVoice|77779855398734| November|   2005|
|       16483|        Vista|11111452278422|September|   2005|
|        4537|        Vista|11118031111452| November|   2005|
|       17085|ColonialVoice|77771842357478|    April|   2005|
|        8933|ColonialVoice|77778597022063| February|   2005|
|       18063|        Vista|11118480983244| November|   2005|
|        9787|        Vista|11117346694204|   August|   2005|
|         686|        Vista|11112478616845| February|   2005|
|        3915|  Distinguish|55559045269754|    April|   2005|
|        8241|ColonialVoice|77779477374895|  January|   2005|
|        4222|        Vista|11119221396064|    April|   2005|
|         828|ColonialVoice|77774359839272|      May|   2005|
|        8188|  Distinguish|55557881290787| February|   2005|
+------------+-------------+--------------+---------+-------+
only showing top 50 rows
