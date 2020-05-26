# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python37_app]
from __future__ import absolute_import

from flask import Flask
import os
from google.cloud import storage
from google.cloud import bigquery
import sys
from googleads import adwords
from google.cloud.exceptions import NotFound
import glob
import gcsfs
import pandas as pd
import pandas_gbq as pd_gbq
from pyarrow import csv

google_application_credentials = 'your_file.json' 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_application_credentials 

app = Flask(__name__)

destination_file_name_01 = 'your_destination_file_name_01.csv'
destination_file_name_02 = 'your_destination_file_name_02.csv'
gcloud_bucket_name = 'your_gcloud_bucket_name' 
dataset_id = 'your_dataset_id'
project_id = 'your_project_id'
gcs = storage.Client()
bucket = gcs.get_bucket(gcloud_bucket_name)
client_bigquery = bigquery.Client()
dataset_ref = client_bigquery.dataset(dataset_id)

# Location of big query tables 
table_id_01 = 'google_shopping_performance_table_01'
table_id_02 = 'google_shopping_performance_table_02'
table_ref_01 = dataset_ref.table(table_id_01)
table_ref_02 = dataset_ref.table(table_id_02)

# Uri of csv file in cloudstorage
uri_01 = "gs://"+gcloud_bucket_name+"/"+destination_file_name_01
uri_02 = "gs://"+gcloud_bucket_name+"/"+destination_file_name_02

def table_exists(client_bigquery,table_ref):
    try:
        table = client_bigquery.get_table(table_ref)
        if table:
            return True
    except NotFound as error:
        return False
        
def remove_bigquery_tables(table):
    if table_exists(client_bigquery,table):
         client_bigquery.delete_table(table)

def load_to_bq(fs,destination_file_name,dataset_id,table,chunk_row_size=2000000):
    with fs.open(gcloud_bucket_name+'/'+destination_file_name, "rb") as tmp_file:
         table = csv.read_csv(tmp_file)
         df = table.to_pandas()
         df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.','')
         pd_gbq.to_gbq(df,destination_table=dataset_id+'.'+table,project_id=project_id,chunksize=chunk_row_size,if_exists='append')

@app.route('/')
def load_to_bigquery_google_shopping_performance_report():
    fs = gcsfs.GCSFileSystem(project=project_id,token=google_application_credentials)
    # if you don't want to remove existing biqquery tables then comment out 2 below lines 
    remove_bigquery_tables(table_ref_01)
    remove_bigquery_tables(table_ref_02)

    client = adwords.AdWordsClient.LoadFromStorage('./googleads.yaml')
    
    # Initialize appropriate service.
    report_downloader = client.GetReportDownloader(version='v201809')

    # Create report1 query
    report_query_01 =  (
            'SELECT CampaignName, '
                'OfferId,Impressions,Clicks,AverageCpc,Cost,Ctr,Conversions,ConversionValue, '
                'ProductTypeL1,ProductTypeL2,ProductTypeL3,ProductTypeL4,ProductTypeL5, '
                'Date ' 
            'FROM   SHOPPING_PERFORMANCE_REPORT ' 
            'WHERE  CampaignStatus = "ENABLED"  AND AdGroupStatus = "ENABLED" '            
            'DURING LAST_7_DAYS ' 
             )     
     
    # Download report1 to temporary directory     
    with fs.open(gcloud_bucket_name+'/'+destination_file_name_01, "wb") as file1:
                 report_downloader.DownloadReportWithAwql(
                 report_query_01, 'CSV',file1, skip_report_header=True,
                 skip_column_header=False, skip_report_summary=True,
                 include_zero_impressions=False)
                 
    
    # Create report2 query
    report_query_02 =  (
            'SELECT OfferId,ProductTitle,ConversionTypeName,ConversionCategoryName, '
            'ExternalConversionSource ' 
            'FROM   SHOPPING_PERFORMANCE_REPORT ' 
            'WHERE  CampaignStatus = "ENABLED" AND AdGroupStatus = "ENABLED" '            
            'DURING LAST_7_DAYS ' 
             )    

    # Download report2 to temporary directory     
    with fs.open(gcloud_bucket_name+'/'+destination_file_name_02, "wb") as file2:
                 report_downloader.DownloadReportWithAwql(
                 report_query_02, 'CSV', file2, skip_report_header=True,
                 skip_column_header=False, skip_report_summary=True,
                 include_zero_impressions=False)    

    # Load reports to bigquery from cloud storage
    load_to_bq(fs,destination_file_name_01,dataset_id,table_id_01)
    load_to_bq(fs,destination_file_name_02,dataset_id,table_id_02)

    destination_table_01 = client_bigquery.get_table(dataset_ref.table(table_id_01))
    destination_table_02 = client_bigquery.get_table(dataset_ref.table(table_id_02))
                  
    return ("Loaded for table 01 {} rows.\nLoaded for table 02 {} rows".format(destination_table_01.num_rows,destination_table_02.num_rows))              

@app.route('/_ah/warmup')
def warmup():
    # Handle your warmup logic here, e.g. set up a database connection pool
    return '', 200, {}

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]

