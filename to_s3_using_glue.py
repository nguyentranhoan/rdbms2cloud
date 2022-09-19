import datetime
import os
import sys
from datetime import timedelta

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


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


def write_data(data, exe_date, output_path, glue_context):
    glue_context.purge_s3_path(output_path+f"/year={exe_date.year}/month={exe_date.month}/day={exe_date.day}")
    new_data = data. \
        filter(data.year == exe_date.year). \
        filter(data.month == exe_date.month). \
        filter(data.day == exe_date.day)
    if new_data is not None:
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(new_data, glueContext, table),
            connection_type="s3",
            format="glueparquet",
            connection_options={
                "path": output_path,
                "partitionKeys": ["year", "month", "day"],
            },
            transformation_ctx=f"migrating_{table}")


def first_run(data, output_path):
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(data, glueContext, table),
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month", "day"],
        },
        transformation_ctx=f"migrating_{table}")


def run_for_cdc(data, cdc_log_df, table_name: str, output_path, glue_context):  # batching daily
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
    write_data(data, execution_date, output_path, glue_context)
    for i in update_dates:
        write_data(data, i, output_path, glue_context)
    for i in delete_dates:
        write_data(data, i, output_path, glue_context)


if __name__ == "__main__":
    # env variables
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    app_name = f"RDBMS2Cloud"
    table_list = ["customer", "manufacturer", "product", '''"order"''', "order_items"]

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    conf = SparkConf()
    pyspark = SparkSession.builder.appName(
        f"{app_name}").config(conf=conf).getOrCreate()

    # Script
    source_data_cdc = glueContext.create_dynamic_frame.from_options(connection_type='postgresql',
                                                                    connection_options={"url": f"{os.getenv('DB_URL')}",
                                                                                        "user": f"{os.getenv('DB_USER')}",
                                                                                        "password": f"{os.getenv('DB_PW')}",
                                                                                        "dbtable": "audit.logged_actions",
                                                                                        "redshiftTmpDir": "s3://demo-create-bucket-tree/temp/"},
                                                                    transformation_ctx="cdc")
    source_data_cdc_as_df = source_data_cdc.toDF()

    for table in table_list:
        output = "s3://demo-create-bucket-tree/" + table
        source_data_db = glueContext.create_dynamic_frame.from_options(connection_type='postgresql',
                                                                       connection_options={
                                                                           "url": f"{os.getenv('DB_URL')}",
                                                                           "user": f"{os.getenv('DB_USER')}",
                                                                           "password": f"{os.getenv('DB_PW')}",
                                                                           "dbtable": table,
                                                                           "redshiftTmpDir": "s3://demo-create-bucket-tree/temp/"},
                                                                       transformation_ctx="source_data")
        source_data_db_as_df = source_data_db.toDF()

        first_run(source_data_db_as_df, output)
        run_for_cdc(source_data_db_as_df, source_data_cdc_as_df, table, output, glueContext)

    # Commit job
    job.commit()
