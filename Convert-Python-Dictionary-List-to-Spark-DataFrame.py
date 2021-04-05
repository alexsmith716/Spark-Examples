from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from decimal import Decimal

# Convert Python Dictionary List to Spark DataFrame

appName = "Convert Python Dictionary List to Spark DataFrame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
	.appName(appName) \
	.master(master) \
	.getOrCreate()

# dictionary List
data = [{"Category": 'Category A', 'ItemID': 1, 'Amount': 12.40},
	{"Category": 'Category B', 'ItemID': 2, 'Amount': 30.10},
	{"Category": 'Category C', 'ItemID': 3, 'Amount': 100.01},
	{"Category": 'Category A', 'ItemID': 4, 'Amount': 110.01},
	{"Category": 'Category B', 'ItemID': 5, 'Amount': 70.85}
	]

# DataFrame can be directly created from Python dictionary list and the schema will be inferred automatically
def infer_schema():
	# Create data frame
	df = spark.createDataFrame(data)
	print(df.schema)
	df.show()

# StructType(List(StructField(Amount,DoubleType,true),StructField(Category,StringType,true),StructField(ItemID,LongType,true)))
#
# 21/04/04 20:47:04 INFO CodeGenerator: Code generated in 13.133547 ms
# +------+----------+------+
# |Amount|  Category|ItemID|
# +------+----------+------+
# |  12.4|Category A|     1|
# |  30.1|Category B|     2|
# |100.01|Category C|     3|
# |110.01|Category A|     4|
# | 70.85|Category B|     5|
# +------+----------+------+

# define the schema directly when creating the data frame
# control the data types explicitly
def explicit_schema():
	# Create a schema for the dataframe
	schema = StructType([
		StructField('Category', StringType(), False),
		StructField('ItemID', IntegerType(), False),
		StructField('Amount', FloatType(), True)
	])
	# Create data frame
	df = spark.createDataFrame(data, schema)
	print(df.schema)
	df.show()

# StructType(List(StructField(Category,StringType,false),StructField(ItemID,IntegerType,false),StructField(Amount,FloatType,true)))
#
# 21/04/04 20:50:20 INFO CodeGenerator: Code generated in 13.328679 ms
# +----------+------+------+
# |  Category|ItemID|Amount|
# +----------+------+------+
# |Category A|     1|  12.4|
# |Category B|     2|  30.1|
# |Category C|     3|100.01|
# |Category A|     4|110.01|
# |Category B|     5| 70.85|
# +----------+------+------+

if __name__ == "__main__":
	infer_schema()
	explicit_schema()
