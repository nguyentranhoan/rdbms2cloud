# using structured streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
sch = spark.read.parquet("statics/order_items/year=2022/month=7/day=19/part-00000-f555bd6b-69fc-4d36-b07b-b53e38c31282.c000.snappy.parquet").schema

df = spark.read.parquet("statics/order_items/year=2022?/month=7/day=19/part-00000-f555bd6b-69fc-4d36-b07b-b53e38c31282.c000.snappy.parquet").collect()

spark.createDataFrame(df,schema=sch).show()
# df = spark.readStream.schema(sch).parquet(path="statics/order_items")
# # df.writeStream.option("checkpointLocation", "checkpoint/").start(path="statics/temp", outputMode='append')
# # print("done")
# df.writeStream.format('parquet').option("checkpointLocation", "checkpoint/").start(path="s3a://demo-create-bucket-tree/")

