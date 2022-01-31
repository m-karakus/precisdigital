import os, time
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import schedule
import pickle
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from oauth2client.service_account import ServiceAccountCredentials

import io
from pytz import timezone
localtz = timezone('Europe/Istanbul')
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd
import numpy as np
from scipy import stats

pd.options.mode.chained_assignment = None  # default='warn'


def insertintobq_keywordResult(dataset_id,table_name,result):
    KEY_FILE_LOCATION = "key.json"
    
    # establish a BigQuery client
    client = bigquery.Client.from_service_account_json(KEY_FILE_LOCATION)

    # create a job config
    """
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Keyword", "STRING"),
            bigquery.SchemaField("LandingPage", "STRING"),
            bigquery.SchemaField("QuestionId", "INTEGER"),
            bigquery.SchemaField("Title", "STRING"),
            bigquery.SchemaField("SearchTryCount", "INTEGER"),
            bigquery.SchemaField("Ip", "STRING"),
            bigquery.SchemaField("Agent", "STRING"),
            bigquery.SchemaField("ChannelId", "INTEGER"),
            bigquery.SchemaField("reportDate", "TIMESTAMP"),
        ]
    )
    """

    job_config = bigquery.LoadJobConfig()

    # Set the destination table
    table_ref = client.dataset(dataset_id).table(table_name)
    #job_config.destination = table_ref
    # job_config.write_disposition = "WRITE_APPEND"
    job_config.write_disposition = "WRITE_TRUNCATE"
    load_job = client.load_table_from_dataframe(result, table_ref, job_config=job_config)
    load_job.result()
    print("tamamlandı")

def customersInsert():
    global customers
    file = "file_customers.csv"

    headers = ['customer_id', 'customer_city', 'channel']
    dtypes = {'customer_id': 'str', 'customer_city': 'str', 'channel': 'str'}
    parse_dates = []

    customers = pd.read_csv(file, dtype=dtypes, parse_dates=parse_dates)
    
    # Detect and exclude outliers in a pandas DataFrame
    customers = customers.drop_duplicates(keep='first')

    dataset_id = "EditedReports"
    table_name = "Customer"
    insertintobq_keywordResult(dataset_id,table_name,customers)
    return


def ordersInsert():
    global orders
    file = "file_orders.csv"

    # headers = ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_item_id","product_id","seller_id","price","freight_value","currency","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"]
    dtypes = {"order_id":'str',"customer_id":'str',"order_status":'str',"order_item_id":'Int64',"product_id":'str',"seller_id":'str',"price":'float',"freight_value":'float',"currency":'str',"product_category_name":'str',"product_name_lenght":'Int64',"product_description_lenght":'Int64',"product_photos_qty":'Int64',"product_weight_g":'Int64',"product_length_cm":'Int64',"product_height_cm":'Int64',"product_width_cm":'Int64'}
    parse_dates = ["order_approved_at","order_purchase_timestamp","order_delivered_carrier_date"]

    # orders = pd.read_csv(file, sep=',', header=0, names=headers, dtype=dtypes, parse_dates=parse_dates)
    orders = pd.read_csv(file, sep=',', dtype=dtypes, parse_dates=parse_dates)

    # Detect and exclude outliers in a pandas DataFrame  -->productId: 241241093bd6986615756b4d8a01de8f
    # orders["std"]=(np.abs(stats.zscore(orders["price"])))
    # orders.loc[orders['product_id'] == '241241093bd6986615756b4d8a01de8f']
    orders = orders[(np.abs(stats.zscore(orders["price"])) < 0.3)]
    orders = orders.drop_duplicates(keep='first')

    dataset_id = "EditedReports"
    table_name = "Order"
    insertintobq_keywordResult(dataset_id,table_name,orders)
    return


def paymentsInsert():
    global payments
    file = "file_payments.csv"
    dtypes = {"order_id":'str','payment_types':'Int64','payment_method':'str',"payment_value":'float','currency':'str'}
    parse_dates = []

    payments = pd.read_csv(file, sep=',', dtype=dtypes, parse_dates=parse_dates)

    # Detect and exclude outliers in a pandas DataFrame
    payments = payments[(np.abs(stats.zscore(payments["payment_value"])) < 3)]
    payments = payments.drop_duplicates(keep='first')

    dataset_id = "EditedReports"
    table_name = "Payment"
    insertintobq_keywordResult(dataset_id,table_name,payments)
    return


CLIENT_SECRET_FILE = "key.json"
API_SERVICE_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET_FILE, SCOPES)
service = build(API_SERVICE_NAME, API_VERSION, credentials=credentials, cache_discovery=False)


def downloadFile(file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))





def everyMinute():
    page_token = None
    while True:
        response = service.files().list(q="'1IAbub9-LFMMauFUo93w1cgKu9rReECEC' in parents and mimeType='text/csv'",
                                                spaces='drive',
                                                fields='nextPageToken, files(id, name,modifiedTime)',
                                                pageToken=page_token).execute()
        for file in response.get('files', []):
            id = file.get('id')
            name = file.get('name')
            modifiedTime = datetime.strptime(file.get('modifiedTime'), "%Y-%m-%dT%H:%M:%S.%f%z")

            now = localtz.localize(datetime.now())

            duration = now - modifiedTime 
            duration_in_s = duration.total_seconds()  
            # Process change
            # print(f'Found file: id: {id} | Name: {name} | ModifiedTime: {modifiedTime}')
            minutes = int(duration_in_s/60)
            if minutes < 1:
                downloadFile(id)
                print(minutes)

                if name == 'file_payments.csv':
                    paymentsInsert()
                if name == 'file_orders.csv':
                    ordersInsert()
                if name == 'file_customers.csv':
                    customersInsert()
            else:
                print("no changes")
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break  
    return

schedule.every(1).minutes.do(everyMinute)


def main():
    print ("started")
    print( time.strftime('%X %x %Z'))
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(e)
        time.sleep(60)

if __name__ == "__main__":
    main()