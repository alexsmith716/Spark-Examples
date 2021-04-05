from pyspark.sql import SparkSession, Row
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from decimal import Decimal
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType

# Change Column Type in PySpark DataFrame
# convert StringType to DoubleType, StringType to Integer, StringType to DateType

appName = "Change Column Type in PySpark DataFrame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
	.appName(appName) \
	.master(master) \
	.getOrCreate()

# dictionary List
data = [{"Category": 'Category A', "ID": 1, "Value": Decimal(12.40)},
	{"Category": 'Category B', "ID": 2, "Value": Decimal(30.10)},
	{"Category": 'Category C', "ID": 3, "Value": Decimal(100.01)}
	]

# explicitly define the schema for the DataFrame
# define the schema based on the data types in the dictionary
schema = StructType([
	StructField('Category', StringType(), False),
	StructField('ID', IntegerType(), False),
	StructField('Value', DecimalType(scale=2), True)
])

# Create data frame
df = spark.createDataFrame(data, schema)
# add two constant columns via lit function
df1 = df.withColumn('Str_Col1', lit('1')).withColumn('Str_Col2', lit('2021-04-04'))
# change column types using cast function
# function DataFrame.cast can be used to convert data types
df1 = df1.withColumn("Str_Col1_Int", df1['Str_Col1'].cast('int')).drop('Str_Col1') \
    .withColumn('Str_Col2_Date', df1['Str_Col2'].cast(DateType())).drop('Str_Col2')
df1.show()
print(df1.schema)

# add two constant columns via lit function:
# ==================================================
# StructType(List(StructField(Category,StringType,false),StructField(ID,IntegerType,false),StructField(Value,DecimalType(10,2),true),StructField(Str_Col1,StringType,false),StructField(Str_Col2,StringType,false)))
#
# 21/04/04 22:11:54 INFO CodeGenerator: Code generated in 15.579923 ms
# +----------+---+------+--------+----------+
# |  Category| ID| Value|Str_Col1|  Str_Col2|
# +----------+---+------+--------+----------+
# |Category A|  1| 12.40|       1|2021-04-04|
# |Category B|  2| 30.10|       1|2021-04-04|
# |Category C|  3|100.01|       1|2021-04-04|
# +----------+---+------+--------+----------+

# change column types using cast function:
# ==================================================
# StructType(List(StructField(Category,StringType,false),StructField(ID,IntegerType,false),StructField(Value,DecimalType(10,2),true),StructField(Str_Col1_Int,IntegerType,true),StructField(Str_Col2_Date,DateType,true)))
#
# 21/04/04 22:19:34 INFO CodeGenerator: Code generated in 16.271092 ms
# +----------+---+------+------------+-------------+
# |  Category| ID| Value|Str_Col1_Int|Str_Col2_Date|
# +----------+---+------+------------+-------------+
# |Category A|  1| 12.40|           1|   2021-04-04|
# |Category B|  2| 30.10|           1|   2021-04-04|
# |Category C|  3|100.01|           1|   2021-04-04|
# +----------+---+------+------------+-------------+
