# # import json
# # import os
# # import datetime
# #
# # from dotenv import load_dotenv
# # from pyspark import SparkConf
# # from pyspark.sql import SparkSession
# #
# # load_dotenv()
# #
# # if __name__ == "__main__":
# #     conf = SparkConf()
# #     conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# #     pyspark = SparkSession \
# #         .builder \
# #         .appName("Python Spark SQL basic example") \
# #         .config("spark.jars", "spark/postgresql-42.5.0.jar") \
# #         .getOrCreate()
# #     df = pyspark.read \
# #         .format("jdbc") \
# #         .option("url", f"{os.getenv('DB_URL')}") \
# #         .option("dbtable", '''"customer"''') \
# #         .option("user", f"{os.getenv('DB_USER')}") \
# #         .option("password", f"{os.getenv('DB_PW')}") \
# #         .option("driver", "org.postgresql.Driver") \
# #         .load()
# #     # df.write.parquet(path="statics/somewhere", partitionBy=["year", "month", "day"])
# #     # df.select("created_at").orderBy("created_at").show()
# #     res_creation = df.agg({"created_at": "max"}).toJSON().top(1)[0]
# #     res_update = df.agg({"updated_at": "max"}).toJSON().top(1)[0]
# #     tms_creation = json.loads(res_creation)["max(created_at)"]
# #     tms_update = json.loads(res_update)["max(updated_at)"]
# #     cdc = df.filter(f"updated_at>{tms_creation}")
# #     cdc.show()
# #     cdc.write.parquet(path="statics/somewhere", partitionBy=["year", "month", "day"], mode='overwrite')
# #     # df.show()
# #     # df.write.parquet(path="statics/somewhere", partitionBy=["year", "month", "day"])
# #     # df.write.parquet(path="static/somewhere", partitionBy="customer_id")
# #
#
#
# # This is a sample Python script.
#
# # Press ⌃R to execute it or replace it with your code.
# # Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# import psycopg2
# import datetime
# from datetime import timedelta
#
#
# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     pass
#
#
# def x_date(x):
#     last_x_date = datetime.datetime.now() - timedelta(x)
#     return int(last_x_date.timestamp())
#
#
# def x_dates(x):
#     last_x_date = datetime.datetime.now() - timedelta(x)
#     return last_x_date
#
#
# def get_db_connection():
#     conn = psycopg2.connect(dbname='rdbms2cloud',
#                             user='postgres',
#                             password='123444',
#                             host='localhost',
#                             port='5432')
#     cur = conn.cursor()
#     for table in ["order_items", "order", "manufacturer", "customer", "product"]:
#         # cur.execute(f"""update "{table}" set created_at={x_date(49)} where id<10000;""")
#         # cur.execute(f"""update "{table}" set created_at={x_date(39)} where id>10000 and id<20000;""")
#         # cur.execute(f"""update "{table}" set created_at={x_date(29)} where id>20000 and id<40000;""")
#         # cur.execute(f"""update "{table}" set created_at={x_date(19)} where id>40000 and id<60000;""")
#         # cur.execute(f"""update "{table}" set created_at={x_date(9)} where id>60000 and id<80000;""")
#         # cur.execute(f"""update "{table}" set updated_at={x_date(49)} where id<10000;""")
#         # cur.execute(f"""update "{table}" set updated_at={x_date(39)} where id>10000 and id<20000;""")
#         # cur.execute(f"""update "{table}" set updated_at={x_date(29)} where id>20000 and id<40000;""")
#         # cur.execute(f"""update "{table}" set updated_at={x_date(19)} where id>40000 and id<60000;""")
#         # cur.execute(f"""update "{table}" set updated_at={x_date(9)} where id>60000 and id<80000;""")
#         cur.execute(
#             f"""update "{table}" set year={x_dates(49).year}, month={x_dates(49).month}, day={x_dates(49).day} where id<10000;""")
#         cur.execute(
#             f"""update "{table}" set year={x_dates(39).year}, month={x_dates(39).month}, day={x_dates(39).day} where id>10000 and id<20000;""")
#         cur.execute(
#             f"""update "{table}" set year={x_dates(29).year}, month={x_dates(29).month}, day={x_dates(29).day} where id>20000 and id<40000;""")
#         cur.execute(
#             f"""update "{table}" set year={x_dates(19).year}, month={x_dates(19).month}, day={x_dates(19).day} where id>40000 and id<60000;""")
#         cur.execute(
#             f"""update "{table}" set year={x_dates(9).year}, month={x_dates(9).month}, day={x_dates(9).day} where id>60000 and id<80000;""")
#     conn.commit()
#     cur.close()
#     conn.close()
import datetime
import json
import os
import datetime
from datetime import timedelta
from dotenv import load_dotenv
import functools
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_connection():
    conf = SparkConf()
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    pyspark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "postgresql-42.5.0.jar") \
        .getOrCreate()

    return pyspark


def get_execution_date(last_x_date: int):
    execution_date = datetime.datetime.now() - datetime.timedelta(last_x_date)
    exe_min = datetime.datetime(year=execution_date.year,
                                month=execution_date.month,
                                day=execution_date.day,
                                hour=0, minute=0, second=0, microsecond=0)
    exe_max = datetime.datetime(year=execution_date.year,
                                month=execution_date.month,
                                day=execution_date.day,
                                hour=23, minute=59, second=59, microsecond=999)
    max_ts = int(datetime.datetime.timestamp(exe_max))
    min_ts = int(datetime.datetime.timestamp(exe_min))
    return execution_date.date(), max_ts, min_ts


# Press the green button in the gutter to run the script.
def alo_test(z, x, y):
    return x + y * z


if __name__ == '__main__':
    pyspark = get_spark_connection()

    df = pyspark.read.parquet("statics/product/year=2022/month=9/day=18/part-00000-aa76abe5-8812-41e9-b71e-881953f73bf7.c000.snappy.parquet")
    df.write.parquet(path="s3a://demo-create-bucket-tree/")



