# code to remove duplicate klaviyo consent customer from consent backfill dataframe.



import json

import boto3

from awsglue.context import GlueContext
from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import udf
from uuid import uuid4
from datetime import datetime, timedelta

import time
import pytz
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DoubleType,
    MapType,
)
import pyspark.sql.functions as F
from pyspark.sql.functions import struct, expr, when, col
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

args = getResolvedOptions(
    sys.argv,
    [
        "CURATED_DATA_PATH",
        "XDM_DATA_PATH",
        "DATABASE_NAME",
        "STORE_DOMAIN",
        "CUSTOMER_CONSENT_TABLE",
        "CUSTOMER_BACKFILL_TABLE",
        "KLAVIYO_PROFILES_TABLE"
    ],
)
store_domain = args["STORE_DOMAIN"]
# workflow_name = args['WORKFLOW_NAME']
# workflow_run_id = args['WORKFLOW_RUN_ID']
glue_client = boto3.client("glue")
# workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
#                                        RunId=workflow_run_id)["RunProperties"]
# print(workflow_params)
# store_domain = workflow_params['store_domain']
customer_consent_tables = args["CUSTOMER_CONSENT_TABLE"]
customer_backfill_tables = args["CUSTOMER_BACKFILL_TABLE"]
klaviyo_profiles_tables = args["KLAVIYO_PROFILES_TABLE"]
curated_data_path = args["CURATED_DATA_PATH"]
xdm_data_path = args["XDM_DATA_PATH"]
database_name = args.get("DATABASE_NAME")
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

consent_preference_col_list = ["baby_preference", "skin_preference", "cosmetics_preference", "email",
                                   "hashed_email", "ingested_at", "marketing_consent", "x_shopify_shop_domain", ]


def remove_duplicate_consent_customer(backfill_consent_df,customer_consent_df):

    customer_consent_df = customer_consent_df.withColumn("email_updated_combo",F.concat_ws('-',col("email"),col("updated_at"),col("timestamp"),col("marketing_consent")))
    backfill_consent_df = backfill_consent_df.withColumn("email_updated_combo",F.concat_ws('-',col("email"),col("updated_at"),col("timestamp"),col("marketing_consent")))
    backfill_consent_df = backfill_consent_df.join(customer_consent_df,backfill_consent_df.email_updated_combo==customer_consent_df.email_updated_combo,"left_anti")
    # backfill_consent_df = backfill_consent_df.drop(col("email_updated_combo"))
    backfill_consent_df = backfill_consent_df.withColumn("consentPreference", F.struct(
            *consent_preference_col_list))
    backfill_consent_df = backfill_consent_df.withColumn("_coty", F.struct('consentPreference'))

    backfill_consent_df = backfill_consent_df.withColumn("eventType",
                                                           F.when(F.col("created_at") == F.col("updated_at"),
                                                                  F.lit('created/klaviyo_profiles')).otherwise(
                                                               F.lit('updates/klaviyo_profiles')))
    backfill_consent_df = backfill_consent_df.withColumn("_id", F.concat_ws("_", F.col("id"), F.col(
            "_coty.consentPreference.ingested_at")))

    print(":::::::::::writing curated:::::::::::::::::::::::")

    curated_destination = curated_data_path + "dataset=shopify@" + store_domain + "/"
    backfill_consent_df = backfill_consent_df.dropDuplicates()
    backfill_consent_curated_df = backfill_consent_df.select(*consent_preference_col_list,'timestamp','eventType','created_at','updated_at')

    print(":::::::::::: curated SChema::::::::::")
    backfill_consent_curated_df.printSchema()


    backfill_consent_curated_df.write.option("maxRecordsPerFile", 300000).mode("append").json(
            curated_destination)
    print("customer consent curated payloads saved to ", curated_destination)
    """
     Creating XDM format
     _coty contains all required columns
    """

    finalcolumn = ["_coty", 'eventType', 'timestamp', '_id']
    xmd_df = backfill_consent_curated_df.select(*finalcolumn)
    # print(":::::::::::: xdm_df SChema::::::::::")
    # xmd_df.printSchema()
    destination = xdm_data_path + "dataset=shopify@" + store_domain + "/"
    xmd_df.write.option("maxRecordsPerFile", 300000).mode("append").json(destination)

if __name__ == "__main__":
    print(":::::::calling main:::::::::")

    print("::::::::: Creating Backfill Dynamic Frame::::::::")
    backfill_consent_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=customer_backfill_tables,
        transformation_ctx = "datasource0")

    backfill_consent_df = backfill_consent_dyf.toDF()
    print(":::::backfill_consent_df  completed:::::::::::")
    backfill_consent_df.printSchema()
    print("::::::::: Creating Customer consent Dynamic Frame::::::::")
    push_down_predicate_customer_consent = f"(dataset = 'shopify@{store_domain}')"
    print("push_down_predicate_customer_consent", push_down_predicate_customer_consent)
    customer_consent_dyf = glueContext.create_dynamic_frame.from_catalog(database=database_name,
                                                                         table_name=customer_consent_tables,
                                                                         push_down_predicate=push_down_predicate_customer_consent,
                                                                         transformation_ctx="datasource0", )
    print(":::::customer df completed:::::::::::")
    customer_consent_df = customer_consent_dyf.toDF()

    uuidUdf = udf(lambda: str(uuid4()), StringType())
    backfill_consent_df = backfill_consent_df.withColumn("id", uuidUdf())

    customer_consent_df.printSchema()
    print(":::::::::::::::::::calling remove_duplicate_consent_customer :::::::::::::")
    remove_duplicate_consent_customer(backfill_consent_df, customer_consent_df)