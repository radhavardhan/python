import time
import boto3
import psycopg2
import os

# import awswrangler as wr
DATABASE = 'shopify'
output = 's3://scheduled-script-output'
client = boto3.client('athena')
SNS_TOPIC_ARN = os.environ['SNS_SCRIPT_TOPIC']
sns_client = boto3.client('sns')


# df = wr.s3.read_excel(path=s3_uri)


def lambda_handler(event, context):
    # print("event",event)
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # response = s3.get_object(Bucket=bucket, Key=key)
    # bucket_data = response['Body'].read().decode("utf-8")
    # print("bucket_data",bucket_data)

    credential = {}
    pwd1 = "eNqrVkrMSVeyUgoKNjI1U9JRqjAtBfIyc4vj8xINdbNTK3UTS3QN9ZJTi4CS2ZkpWCSBEpklJUCJxBKlWgC4CRff.eNqNUl1vmzAU_S9-BmLznUiTRgJN25XQsKxZq0jI2CZzFmwETpa26n-fIeke2pfZL_cen3t9P84r4BRMAPK9IAjcsROMvaCofMeuHD80SYiw6drUM0Nkl2aFYYUIC1nFcHFgCBhAPTdMx2NCWNcVSv5mQqNkz5lQxZCanUiBm0ajh461ZyzyncCDkevDOI7imaPNsXc1i79GVJbsJtbkTmHVZ37dgF235-WRtRsw2YCjbR6h5SALmra5RSzE-m6AsQFCCsIGjo0c6KLAd33Ptj3kDM-dLpBLMRB-KdV0k9GI150pMLJw_-1ebrmwiKx7eHShj45olK0XdRZvT-nuRqWr6W4xgzBdEedunbjZ_IfG8vpp_sAf69R5Wt3Wd9_RdLFLncVqOl3Ej3_yJJ_mSfqSJgnMVg_zJcyj5To_PtYPt3m0AW-6XdzpXi_l9C7-70ER1YAJNEC11fz16i5zF9-Sn_dJcHO9zpJsebW8Xkbz9Mtw-sF-WLjjhUWF8RhVdmW6xIOmWzrQDJmHTFpCSG06JqFDLwuvJdfhpEQBQuMeaEqsAW2wU8Nb1hVcaD_0XdifvsCW6VXSAquPQuurIfIsoNKqscBbZmBKudKTx3udqZLWTpZFdRCkxz49Nq3cMaKz37eSHoiaSaHYSX3itXLPuo-ocRmiMayfSy1T_m5bWrIt2_JOtc-WboAa-EC1qAk7l9kO5KE1fmQF2csDNeqmM3Q7glOjDylku8WCv-D-z-4MNcTCpPln0xoXCrcKi3fIuAgPvP0FQoslHg==.eNoBAAH__jMo_fGs_fXJELb_ntV6YKSU3Syds8oeMF5vKizbQ6e04wLEbALQoD1NxVM2hIqZ-L0wO0r64OGO_sjQua79_xj9uSQlkxIN3dVgft1cR4gyoKaaiMvVxiiMacgSPCxkSdnxclRtpHSwg4Qgup9o4qOJsflk9HDe734hHon4KnhrXNIGUixJXenX2FcTkg0aS0JrejxnKnSV6pQQ5DbLBAqKn6fLE4FMkHDxHnhTvdnzuNVCD8cN29UqWzCO6fXyoDOphhqASxGUGIdKpExz0lWbtVAUYVnGzSqlS3yz7VffNwF232yPXrb2BCDSmlUF4YW5KPKbEtGIdC8eTz8NrncaBYJD"
    credential['username'] = "157D1990530FC26A0A490D4C@AdobeOrg"
    credential['password'] = pwd1
    credential['host'] = "coty.platform-query.adobe.io"
    credential['db'] = "capdev:all"
    connection = None
    result = []
    try:

        query_athena = 'select count(*) from curated_customers_newdata'

        response = client.start_query_execution(
            QueryString=query_athena,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': output,

            }
        )

        query_id = response['QueryExecutionId']
        print("query_id", query_id)
        time.sleep(20)

        response_status = client.get_query_execution(QueryExecutionId=query_id)

        # print("check status", response_status['QueryExecution']['Status']['State'])
        query_execution_status = response_status['QueryExecution']['Status']['State']
        if query_execution_status == 'SUCCEEDED':
            print(query_execution_status)
            results_athena_print = client.get_query_results(QueryExecutionId=query_id)

            print("check", results_athena_print)
            result_athena = results_athena_print['ResultSet']['Rows'][1]['Data']
            print("ra", result_athena)
            # for row in results_athena_print['ResultSet']['Rows']:

            #     print("test_result",row)
        else:
            print('killed')
            client.stop_query_execution(QueryExecutionId=query_id)

        connection = psycopg2.connect(user=credential['username'], password=credential['password'],
                                      host=credential['host'],
                                      database=credential['db'])

        cursor = connection.cursor()
        query = 'select count(*) from cust'
        # query = "select _coty.subscriptionorder.x_shopify_shop_domain,date(_coty.subscriptionorder.ingested_at),count(*) from coty_subscription_order_event_dataset where date(_coty.subscriptionorder.ingested_at)= current_date - interval '1' day group by 1,2"
        cursor.execute(query)
        data = cursor.fetchall()

        for line in data:
            result.append(line)

        cursor.close()

        print("AEP_result ", data)
        # if data == test_result1:
        #     print("yes")
        # else:
        #     print("no")

        return result


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()









































