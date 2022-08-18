import sys
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
	.builder \
	.appName("Spark Application with AWS EMR") \
	.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

s3Bucket = sys.argv[1]
s3File = sys.argv[2]
s3Path = f"s3://{s3Bucket}/{s3File}"
print(f"s3Bucket: {s3Bucket}")
print(f"s3File: {s3File}")
print(f"s3Path: {s3Path}")
print(f"s3File: {s3File}")

schema = StructType().add("Date", StringType()) \
			.add("Time", StringType()) \
			.add("CO_(mg/m3)", DoubleType()) \
			.add("PT08.S1(CO)", IntegerType()) \
			.add("NMHC_(microg/m3)", IntegerType()) \
			.add("C6H6_(microg/m3)", DoubleType()) \
			.add("NOx_(ppb)", IntegerType()) \
			.add("NO2_(microg/m3)", IntegerType())

df = spark.read.schema(schema).csv(s3Path)
df.show()
print(f"Count: {df.count()}")

df.createOrReplaceTempView("air_pollution")
avgDF = spark.sql("""select \
                        date,\
                        round(avg(`CO_(mg/m3)`), 2)  as avg_CO, \
                        round(avg(`PT08.S1(CO)`), 2) as avg_PT08S1, \
                        round(avg(`NMHC_(microg/m3)`), 2) as avg_NMHC, \
                        round(avg(`C6H6_(microg/m3)`), 2) as avh_C6H6, \
                        round(avg(`NOx_(ppb)`), 2) as avg_NOx, \
                        round(avg(`NO2_(microg/m3)`), 2) as avg_NO2 \
                        from air_pollution \
                        group by date""")

avgDF.show()
avgDF.coalesce(1).write.csv("s3://air-pollution-processed-data/", mode="append")
print(f"Count: {avgDF.count()}")
