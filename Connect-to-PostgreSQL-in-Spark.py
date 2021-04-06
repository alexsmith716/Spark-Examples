import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Connect to PostgreSQL in Spark (PySpark)

# pg_ctl -D /usr/local/var/postgres start
# pg_ctl -D /usr/local/var/postgres stop
# psql -l
# createdb testdb
# dropdb testdb
# psql testdb
# testdb=# \dt

# Host: localhost or 127.0.0.1
# Port: 5432
# Database: testdb

# MacBook-Pro ~ % psql -l
#                                 List of databases
#    Name    |   Owner    | Encoding | Collate | Ctype |     Access privileges     
# -----------+------------+----------+---------+-------+---------------------------
#  testdb    | MacBookPro | UTF8     | C       | C     | 

# # construct a pandas DataFrame in memory and then save the result to PostgreSQL database
# data = [{"id": 1, "value": 'ABC'},{"id": 2, "value": 'DEF'}]
# pdf = pd.DataFrame(data)
# print(pdf)
# # create SQLAlchemy engine
# engine = create_engine("postgresql+psycopg2://@localhost:5432/testdb?client_encoding=utf8")
# # save result to the database via engine
# pdf.to_sql('test_table', engine, index=False, if_exists='replace')

# 21/04/05 22:15:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
#    id value
# 0   1   ABC
# 1   2   DEF

appName = "Connect to PostgreSQL in Spark (PySpark)"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

engine = create_engine("postgresql+psycopg2://localhost:5432/testdb?client_encoding=utf8")
pdf = pd.read_sql('select * from test_table', engine)

# Convert Pandas dataframe to spark DataFrame
df = spark.createDataFrame(pdf)
print(df.schema)
df.show()

# StructType(List(StructField(id,LongType,true),StructField(value,StringType,true)))

# 21/04/05 22:17:39 INFO CodeGenerator: Code generated in 12.056241 ms
# +---+-----+
# | id|value|
# +---+-----+
# |  1|  ABC|
# |  2|  DEF|
# +---+-----+
