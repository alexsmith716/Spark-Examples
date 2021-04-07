import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Connect to PostgreSQL AWS instance in Spark (PySpark)
# ******* Just got my AWS service up and running -VERY COOL (docs are working great) *******
# AWS Basics 101: To connect to your AWS PostgreSQL instance >>>> Set-Up Security Group Access Rules <<<<
# Provide access to your DB instance in your VPC by creating a security group
# https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html#CHAP_SettingUp.SecurityGroup

# Host: database-1.dBInstanceEndpoint.us-east-1.rds.amazonaws.com
# Port: 5432 (port will default to the PostgreSQL default port -which is 5432)
# User: dBInstanceUSER
# Password: dBInstancePASSWORD
# Database: dBInstanceNAME

# MacBook-Pro ~ % psql --host database-1.dBInstanceEndpoint.us-east-1.rds.amazonaws.com --username dBInstanceUSER --dbname dBInstanceNAME
# Password for user dBInstanceUSER: 
# psql (13.2, server 12.5)
# SSL connection (protocol: , cipher: , bits: , compression: )
# Type "help" for help.

# dBInstanceNAME-> \l
#                                              List of databases
#    Name            |    Owner       | Encoding |   Collate   |    Ctype    |      Access privileges      
# -------------------+----------------+----------+-------------+-------------+-----------------------------
#  dBInstanceNAME    | dBInstanceUSER | UTF8     | en_US.UTF-8 | en_US.UTF-8 | 
#  postgres          | dBInstanceUSER | UTF8     | en_US.UTF-8 | en_US.UTF-8 | 

# ===========================================

# # construct a pandas DataFrame in memory and then save the result to PostgreSQL database
# data = [{"id": 3, "value": 'ABC'},{"id": 4, "value": 'DEF'}]
# pdf = pd.DataFrame(data)
# print(pdf)
# # create SQLAlchemy engine
# engine = create_engine("postgresql+psycopg2://dBInstanceUSER:dBInstancePASSWORD@database-1.dBInstanceEndpoint.us-east-1.rds.amazonaws.com/dBInstanceNAME?client_encoding=utf8")
# # save result to the database via engine
# pdf.to_sql('test_table', engine, index=False, if_exists='replace')

# 21/04/07 12:14:25 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
#    id value
# 0   1   ABC
# 1   2   DEF

appName = "Connect to PostgreSQL in Spark (PySpark)"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

engine = create_engine("postgresql+psycopg2://dBInstanceUSER:dBInstancePASSWORD@database-1.dBInstanceEndpoint.us-east-1.rds.amazonaws.com/dBInstanceNAME?client_encoding=utf8")
pdf = pd.read_sql('select * from test_table', engine)

# Convert Pandas dataframe to spark DataFrame
df = spark.createDataFrame(pdf)
print(df.schema)
df.show()

# StructType(List(StructField(id,LongType,true),StructField(value,StringType,true)))

# 21/04/07 12:16:45 INFO CodeGenerator: Code generated in 12.207634 ms
# +---+-----+
# | id|value|
# +---+-----+
# |  1|  ABC|
# |  2|  DEF|
# +---+-----+
