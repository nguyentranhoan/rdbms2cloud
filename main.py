import datetime
import os
from datetime import timedelta
import to_s3_using_boto3
from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession

load_dotenv()


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
    execution_date = datetime.datetime.now() - timedelta(last_x_date)
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


def get_df_from_database(table_name: str):
    pyspark = get_spark_connection()
    data = pyspark.read \
        .format("jdbc") \
        .option("url", f"{os.getenv('DB_URL')}") \
        .option("dbtable", f"{table_name}") \
        .option("user", f"{os.getenv('DB_USER')}") \
        .option("password", f"{os.getenv('DB_PW')}") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    return data


def get_cdc_id_list(table_name: str, data, action: str) -> list:
    _, max_ts, min_ts = get_execution_date(last_x_date=0)
    action_data = data.filter(data.table_name == table_name). \
        filter(data.action == action). \
        filter(data.action_tstamp_tx >= min_ts). \
        filter(data.action_tstamp_tx <= max_ts). \
        select('row_data').distinct()
    return action_data.collect()


def get_cdc_ids(cdc_log_df, table_name):
    cdc_insert_ids = get_cdc_id_list(table_name, cdc_log_df, "I")
    cdc_update_ids = get_cdc_id_list(table_name, cdc_log_df, "U")
    cdc_delete_ids = get_cdc_id_list(table_name, cdc_log_df, "D")

    insert_ids = [x["row_data"] for x in cdc_insert_ids if x not in cdc_delete_ids]
    update_ids = [x["row_data"] for x in cdc_update_ids if x not in insert_ids and x not in cdc_delete_ids]
    delete_ids = [x["row_data"] for x in cdc_delete_ids]

    return update_ids, delete_ids


def write_data(data, table_name, exe_date):
    new_data = data. \
        filter(data.year == exe_date.year). \
        filter(data.month == exe_date.month). \
        filter(data.day == exe_date.day)
    if new_data is not None:
        new_data.write.parquet(path=f"statics/{table_name}",
                               partitionBy=["year", "month", "day"],
                               mode='overwrite')
    file_path = f"statics/{table_name}/year={exe_date.year}/month={exe_date.month}/day={exe_date.day}"
    return file_path


def first_run(table_name: str):
    data = get_df_from_database(table_name)
    data.write.parquet(path=f"statics/{table_name}", partitionBy=["year", "month", "day"], mode='overwrite')
    to_s3_using_boto3.upload_to_s3(f"statics/{table_name}")


def run_for_cdc(data, cdc_log_df, table_name: str):  # batching daily
    execution_date, max_ts, min_ts = get_execution_date(last_x_date=0)
    update_ids, delete_ids = get_cdc_ids(cdc_log_df, table_name)
    update_dates = []
    delete_dates = []
    for uid in update_ids:
        uts = data.filter(data.id == uid).select("updated_at").collect()[0]["updated_at"]
        ud = datetime.datetime.fromtimestamp(uts).date()
        update_dates.append(ud)
    for did in delete_ids:
        dts = cdc_log_df.filter(cdc_log_df.row_data == did). \
            filter(cdc_log_df.action == 'D'). \
            select("changed_fields").collect()[0]["changed_fields"]
        dd = datetime.datetime.fromtimestamp(dts).date()
        delete_dates.append(dd)

    update_dates = set(update_dates)
    delete_dates = set(delete_dates)

    # for insert new data
    data_inserted = write_data(data, table_name, execution_date)
    to_s3_using_boto3.upload_to_s3(data_inserted)

    # insert_data = data. \
    #     filter(data.year == execution_date.year). \
    #     filter(data.month == execution_date.month). \
    #     filter(data.day == execution_date.day)
    # if insert_data is not None:
    #     insert_data.write.parquet(path=f"statics/{table_name}",
    #                               partitionBy=["year", "month", "day"],
    #                               mode='overwrite')
    # for update data
    for i in update_dates:
        data_updated = write_data(data, table_name, i)
        to_s3_using_boto3.upload_to_s3(data_updated)
        # update_data = data. \
        #     filter(data.year == i.year). \
        #     filter(data.month == i.month). \
        #     filter(data.day == i.day)
        # if update_data is not None:
        #     update_data.write.parquet(path=f"statics/{table_name}",
        #                               partitionBy=["year", "month", "day"],
        #                               mode='overwrite')
    for i in delete_dates:
        data_deleted = write_data(data, table_name, i)
        to_s3_using_boto3.upload_to_s3(data_deleted)
        # delete_data = data. \
        #     filter(data.year == i.year). \
        #     filter(data.month == i.month). \
        #     filter(data.day == i.day)
        # if delete_data is not None:
        #     delete_data.write.parquet(path=f"statics/{table_name}",
        #                               partitionBy=["year", "month", "day"],
        #                               mode='overwrite')


if __name__ == "__main__":
    cdc_df = get_df_from_database("audit.logged_actions")
    for table in ["customer", "manufacturer", "product", '''"order"''', "order_items"]:
        table_df = get_df_from_database(table)
        # first_run(table)
        run_for_cdc(table_df,cdc_df, table)
