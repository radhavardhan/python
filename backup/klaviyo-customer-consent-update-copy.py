# code for klaviyo profile consent customer
import json

import boto3

from awsglue.context import GlueContext
from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions
import sys
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
        "SECRET_NAME",
        "DURATION",
        "START_DATE",
        "END_DATE",
        "KLAVIYO_PROFILES_TABLE",
        "CURATED_DATA_PATH",
        "XDM_DATA_PATH",
        "DATABASE_NAME",
        "STORE_DOMAIN",
        "CUSTOMER_CONSENT_TABLE",
        # 'WORKFLOW_NAME',
        # 'WORKFLOW_RUN_ID'
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
klaviyo_profiles_tables = args["KLAVIYO_PROFILES_TABLE"]
customer_consent_tables = args["CUSTOMER_CONSENT_TABLE"]
duration = args["DURATION"]
curated_data_path = args["CURATED_DATA_PATH"]
xdm_data_path = args["XDM_DATA_PATH"]
start_date = args.get("START_DATE")
end_date = args.get("END_DATE")
database_name = args.get("DATABASE_NAME")
secretsmanager = boto3.client("secretsmanager")
get_secret_value_response = secretsmanager.get_secret_value(
    SecretId=args["SECRET_NAME"]
)
secret = get_secret_value_response["SecretString"]
secret_values = json.loads(secret)
house_market_dict = json.loads(secret_values.get("klaviyo-house-market-dict"))
house = house_market_dict[store_domain]["house"]
market = house_market_dict[store_domain]["market"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def get_date_time(start_date: str, end_date: str, time_duration: str) -> str:
    """

    :param start_date: start date in this format 2022-02-15
    :param end_date: end date in this format 2022-02-15
    :param time_duration: in days like 1 days 2 days
    :return: start_date and end date
    """
    try:
        # '2022-02-15'
        start_date_time = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_time = datetime.strptime(end_date, "%Y-%m-%d")
        start_date_str = start_date_time.strftime("%Y-%m-%d")
        end_date_str = end_date_time.strftime("%Y-%m-%d")

        print("Valid date string")
    except ValueError:
        print("Invalid date string getting start date from workflow")
        response = glue_client.get_workflow_run(
            Name=workflow_name,
            IncludeGraph=False,
            RunId=workflow_run_id
        )
        end_date_time = response['Run']['StartedOn']
        end_date_str = end_date_time.strftime("%Y-%m-%d")
        start_date_time = end_date_time - timedelta(
            days=int(time_duration)
        )
        start_date_str = start_date_time.strftime("%Y-%m-%d")

    #     end_date_time = datetime.utcnow()
    #     end_date_str = end_date_time.strftime("%Y-%m-%d")
    #     start_date_time = end_date_time - timedelta(
    #         days=int(time_duration)
    #     )
    #     start_date_str = start_date_time.strftime("%Y-%m-%d")
    # print("start_date", start_date_str)
    # print("end_date", end_date_str)

    return start_date_str, end_date_str


properties_schema = StructType([
    StructField("Accepts Marketing", BooleanType(), nullable=True),
    StructField("Shopify Tags", ArrayType(StringType()), nullable=True),
    StructField("validityTag", StringType(), nullable=True),
    StructField("ipWarmupGroup", StringType(), nullable=True),
    StructField("$consent", ArrayType(StringType()), nullable=True),
    StructField("$consent_timestamp", StringType(), nullable=True),
    StructField("$source", StringType(), nullable=True),
    StructField("acs_consent", StringType(), nullable=True),
    StructField("Preference - Cosmetics", StringType(), nullable=True),
    StructField("Preference - Skin", StringType(), nullable=True),
    StructField("Preference - Baby", StringType(), nullable=True),
    StructField("Exclude Welcome", BooleanType(), nullable=True)
])


def get_latest_consent(start_date: str, end_date: str, time_duration: str) -> None:
    """

    :param start_date:
    :param end_date:
    :param time_duration:
    :return:
    """

    start_date, end_date = get_date_time(start_date, end_date, time_duration)
    consent_preference_col_list = ["baby_preference", "skin_preference", "cosmetics_preference", "email",
                                   "hashed_email", "ingested_at", "marketing_consent", "x_shopify_shop_domain", ]

    push_down_predicate_profile = f"updated_at>='{start_date}' and updated_at<='{end_date}' and house=='{house}' and market =='{market}'"

    print("push_down_predicate profile", push_down_predicate_profile)

    klaviyo_profile_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=klaviyo_profiles_tables,
        push_down_predicate=push_down_predicate_profile,
        transformation_ctx="datasource0",
    )
    print(":::::profile df completed:::::::::::")
    klaviyo_profile_df = klaviyo_profile_dyf.toDF()
    klaviyo_profile_df.show()
    print("count of klaviyo profile", klaviyo_profile_df.count())
    # print("klaviyo emails:  ", klaviyo_profile_df.select("attributes.email").show())
    # push_down_predicate_customer_consent = f"updated_at>='{start_date}' and updated_at<'{end_date}' and x_shopify_shop_domain == '{store_domain}'"
    push_down_predicate_customer_consent = f"(dataset = 'shopify@{store_domain}')"

    print("push_down_predicate_customer_consent", push_down_predicate_customer_consent)

    customer_consent_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=customer_consent_tables,
        push_down_predicate=push_down_predicate_customer_consent,
        transformation_ctx="datasource0",

    )
    print(":::::customer df completed:::::::::::")
    customer_consent_df = customer_consent_dyf.toDF()
    # customer_consent_df.printSchema()
    print("customer consent emails:  ", customer_consent_df.select(col("email")).show())
    customer_consent_df = customer_consent_df.filter(
        (col("updated_at") >= '2023-11-21') & (col("updated_at") <= '2023-11-22'))
    print("count of customer consent", customer_consent_df.count())
    customer_consent_df.show()
    filter_consent = ['SUBSCRIBED', 'UNSUBSCRIBED']
    if klaviyo_profile_df.isEmpty():
        print(":::::::there is no  data in this data range", push_down_predicate_profile)
    else:
        windowSpec = Window.partitionBy("attributes.email").orderBy(col("attributes.updated").desc())
        klaviyo_profile_df = klaviyo_profile_df.filter(
            F.col("attributes.subscriptions.email.marketing.consent").isin(filter_consent))

        # email_list=[]
        klaviyo_profile_df = klaviyo_profile_df.withColumn(
            "row_number", row_number().over(windowSpec)
        )

        klaviyo_profile_df = klaviyo_profile_df.join(customer_consent_df,
                                                     klaviyo_profile_df.attributes.email == customer_consent_df.email,
                                                     "leftanti")
        print("klaviyo df count after join")
        print(klaviyo_profile_df.count())
        # filtering latest updated profiles from klaviyo
        print(":::::::::printing klaviyo data:::::::::::")

        klaviyo_profile_df = klaviyo_profile_df.filter(col("row_number") == 1)

        klaviyo_profile_df = klaviyo_profile_df.withColumn("properties_json", F.to_json(F.col("attributes.properties")))
        # klaviyo_profile_df.select("properties_json").show(truncate=False)
        klaviyo_profile_df = klaviyo_profile_df.withColumn("properties",
                                                           F.from_json(F.col("properties_json"), properties_schema))
        print(klaviyo_profile_df.count())
        klaviyo_profile_df.select("properties").printSchema()

        print("Preference")

        klaviyo_profile_df = klaviyo_profile_df.select(
            F.col("properties.Preference - Cosmetics").alias("cosmetics_preference"),
            F.col("properties.Preference - Baby").alias("baby_preference"),
            F.col("properties.Preference - Skin").alias("skin_preference"), F.col('attributes.email').alias("email"),
            F.col('attributes.subscriptions.email.marketing.consent').alias("marketing_consent"),
            F.col('attributes.created').alias('created_at'), F.col('attributes.updated').alias('updated_at'),
            F.col('id'), F.col('attributes.subscriptions.email.marketing.timestamp').alias('timestamp'))

        print("printing after selecting preference")
        klaviyo_profile_df = klaviyo_profile_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        klaviyo_profile_df = klaviyo_profile_df.withColumn("marketing_consent",
                                                           F.when(F.col("marketing_consent") == 'SUBSCRIBED',
                                                                  F.lit('yes')).otherwise(F.lit('no')))
        klaviyo_profile_df = klaviyo_profile_df.withColumn("x_shopify_shop_domain", F.lit(store_domain))
        klaviyo_profile_df = klaviyo_profile_df.withColumn("ingested_at", F.current_timestamp())
        klaviyo_profile_df = klaviyo_profile_df.withColumn('hashed_email', F.lit(F.sha2(F.lower(F.col("email")), 256)))

        klaviyo_profile_df = klaviyo_profile_df.withColumn("consentPreference", F.struct(
            *consent_preference_col_list

        ))

        klaviyo_profile_df = klaviyo_profile_df.withColumn("_coty", F.struct('consentPreference'))

        klaviyo_profile_df = klaviyo_profile_df.withColumn("eventType",
                                                           F.when(F.col("created_at") == F.col("updated_at"),
                                                                  F.lit('created/klaviyo_profiles')).otherwise(
                                                               F.lit('updates/klaviyo_profiles')))
        klaviyo_profile_df = klaviyo_profile_df.withColumn("_id", F.concat_ws("_", F.col("id"), F.col(
            "_coty.consentPreference.ingested_at")))

        print("writing curated")

        curated_destination = curated_data_path + "dataset=shopify@" + store_domain + "/"

        klaviyo_profile_curated_df = klaviyo_profile_df.select(*consent_preference_col_list, 'timestamp', 'eventType',
                                                               'created_at', 'updated_at')

        print(":::::::::::: curated SChema::::::::::")
        klaviyo_profile_curated_df.printSchema()

        klaviyo_profile_curated_df.write.option("maxRecordsPerFile", 300000).mode("append").json(
            curated_destination
        )
        print("customer historical curated payloads saved to ", curated_destination)
        """
         Creating XDM format
         _coty contains all required columns
        """

        finalcolumn = ["_coty", 'eventType', 'timestamp', '_id']
        xmd_df = klaviyo_profile_df.select(*finalcolumn)
        print(":::::::::::: xmd_df SChema::::::::::")
        xmd_df.printSchema()
        destination = xdm_data_path + "dataset=shopify@" + store_domain + "/"
        xmd_df.write.option("maxRecordsPerFile", 300000).mode("append").json(destination)


if __name__ == "__main__":
    print(":::::::calliogn main:::::::::")
    # get the start time
    st = time.time()

    get_latest_consent(start_date, end_date, duration)
