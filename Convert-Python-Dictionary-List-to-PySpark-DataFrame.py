from pyspark.sql import SparkSession, Row
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from decimal import Decimal

# Convert Python Dictionary List to PySpark DataFrame

appName = "Python Example - PySpark Parsing Dictionary as DataFrame"
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
# use pyspark.sql.Row to parse dictionary item
# use ** to unpack keywords in each dictionary
# df = spark.createDataFrame([Row(**i) for i in data])
df = spark.createDataFrame(data, schema)
print(df.schema)
df.show()

# The script created a DataFrame with inferred schema as:
# StructType(List(StructField(Category,StringType,false),StructField(ID,IntegerType,false),StructField(Value,DecimalType(10,2),true)))

#	21/04/04 17:23:17 INFO CodeGenerator: Code generated in 12.8121 ms
#	+----------+---+------+
#	|  Category| ID| Value|
#	+----------+---+------+
#	|Category A|  1|  12.4|
#	|Category B|  2|  30.1|
#	|Category C|  3|100.01|
#	+----------+---+------+
