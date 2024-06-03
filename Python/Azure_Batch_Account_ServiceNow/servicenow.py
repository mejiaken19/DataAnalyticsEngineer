import json
import os 
import requests
import pandas as pd
import numpy as np
import pytz
import argparse
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import TaskAddParameter, ResourceFile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define command-line arguments
parser = argparse.ArgumentParser(description='Process ServiceNow entity data.')
parser.add_argument('entity', type=str, help='The entity to fetch data from')
# Parse the arguments
args = parser.parse_args()
ENTITY = args.entity

# ServiceNow Details:
SYSTEM = 'snow'
INSTANCE_URL = 'https://company.service-now.com'
USERNAME = ''
PASSWORD = ''
# Azure Batch Account Details:
BATCH_ACCOUNT_NAME = 'etlaccount'
BATCH_ACCOUNT_KEY = ''
BATCH_ACCOUNT_URL = 'https://etlaccount.australiaeast.batch.azure.com'
# Azure Storage Account Details:
STORAGE_ACCOUNT_NAME = 'datawarehouse'
STORAGE_ACCOUNT_KEY = ''
STORAGE_CONTAINER_NAME = 'snow'

current_time_sydney = datetime.now(pytz.timezone('Australia/Sydney'))
current_date_sydney = current_time_sydney.date()
formatted_date = current_date_sydney.strftime("%Y/%m/%d")
formatted_date_file_name = current_date_sydney.strftime("%Y%m%d")

# Function to pull api data
def fetch_data(offset):
    limit = 20000
    print(f"current offset:    {offset}")
    batch_number = offset // limit
    #url = f"{INSTANCE_URL}/api/now/table/{ENTITY}?ORDERBYsys_created_on&sysparm_limit={limit}&sysparm_offset={offset}&sysparm_fields=sys_id"
    url = f"{INSTANCE_URL}/api/now/table/{ENTITY}?ORDERBYsys_created_on&sysparm_limit={limit}&sysparm_offset={offset}"
    headers = {"Content-Type": "application/json"}
    response = requests.get(url, auth=(USERNAME, PASSWORD), headers=headers)
    if response.status_code == 200:
        records = response.json().get('result')
        if not records:
            return False
        
        # Flatten the JSON records using Pandas
        flat_records = pd.json_normalize(records, sep='_')
        # Replace NaN with None
        flat_records = flat_records.replace({np.nan: None})
        # Replace empty strings with None
        flat_records = flat_records.replace("", None)
        flat_records_dict = flat_records.to_dict(orient='records')
        # Save the current batch to a separate JSON file
        with open(f"{ENTITY}_Staging_{batch_number}.json", "w") as json_file:
            json.dump(flat_records_dict, json_file,sort_keys=True)
        print(f"Data stored in {ENTITY}_Staging_{batch_number}.json")
        return True
    else:
        print(f"Error fetching {ENTITY}: {response.status_code}")
        return False

# Function to Combine batches of JSON files
def combine_json_files():
    print(f"Combining files into one json..")
    batch_number = 0
    with open(f"{formatted_date_file_name}_{SYSTEM}_{ENTITY}.json", "w") as combined_file:
        while True:
            file_name = f"{ENTITY}_Staging_{batch_number}.json"
            if not os.path.exists(file_name):
                break
            with open(file_name, "r") as json_file:
                data = json.load(json_file)
                for record in data:
                    json.dump(record, combined_file, default=str)
                    combined_file.write("\n")  # Add newline after each record
            batch_number += 1
    print(f"All data combined into {ENTITY}.json")

# Function to upload file to Azure Storage
def upload_to_blob_storage(file_path, container_name, folder_name=None):
    blob_service_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
                                            credential=STORAGE_ACCOUNT_KEY)
     
    container_client = blob_service_client.get_container_client(container_name)
    if folder_name:
        blob_name = f"{folder_name}/{os.path.basename(file_path)}"
    else:
        blob_name = os.path.basename(file_path)
    blob_client = container_client.get_blob_client(blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

if __name__ == "__main__":
    offsets = [i * 20000 for i in range(5)]  # Initialize the first 5 offsets
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_offset = {executor.submit(fetch_data, offset): offset for offset in offsets}
        batch_number = 4

        
        while future_to_offset:
            for future in as_completed(future_to_offset):
                offset = future_to_offset.pop(future)
                try:
                    if future.result():
                        batch_number += 1
                        next_offset = batch_number * 20000
                        future_to_offset[executor.submit(fetch_data, next_offset)] = next_offset
                        print(f"Current batch number: {batch_number}, Next offset: {next_offset}")

                except Exception as exc:
                    print(f'Offset {offset} generated an exception: {exc}')

    combine_json_files()
    upload_to_blob_storage(f"{formatted_date_file_name}_{SYSTEM}_{ENTITY}.json", STORAGE_CONTAINER_NAME, formatted_date)
    print("Script executed successfully! The data today is: {}".format(formatted_date))
