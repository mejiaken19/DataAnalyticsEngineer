import json
import requests
from flask import Flask, jsonify
import pandas as pd
import logging
import pyodbc

app = Flask(__name__)

# Load configuration from config.json file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Set up logging
logging.basicConfig(filename='api.log', level=logging.ERROR)

# ServiceNow API endpoint
SNOW_API_URL = config['ServiceNow']['API_URL']
SNOW_USERNAME = config['ServiceNow']['USERNAME']
SNOW_PASSWORD = config['ServiceNow']['PASSWORD']

# Azure Synapse SQL Database connection details
SQL_SERVER = config['AzureSynapseSQL']['SERVER']
SQL_DATABASE = config['AzureSynapseSQL']['DATABASE']
SQL_USERNAME = config['AzureSynapseSQL']['USERNAME']
SQL_PASSWORD = config['AzureSynapseSQL']['PASSWORD']
SQL_DRIVER = '{ODBC Driver 17 for SQL Server}'

# Function to fetch data from ServiceNow
def fetch_data_from_servicenow():
    try:
        response = requests.get(SNOW_API_URL, auth=(SNOW_USERNAME, SNOW_PASSWORD))
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json()
        return data
    except requests.exceptions.Timeout:
        logging.error("Request timed out while fetching data from ServiceNow")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching data from ServiceNow: {e}")
        return None

# Function to insert data into Azure Synapse SQL Database
def insert_data_into_sql(data):
    try:
        conn_str = f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        for record in data:
            cursor.execute("INSERT INTO SLA (task_id, start_time, end_time, status) VALUES (?, ?, ?, ?)", 
                           (record['task_id'], record['start_time'], record['end_time'], record['status']))
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"An error occurred while inserting data into Azure Synapse SQL Database: {e}")

# API endpoint to fetch data
@app.route('/get_data', methods=['GET'])
def get_data():
    data = fetch_data_from_servicenow()
    if data:
        insert_data_into_sql(data)
        df = pd.DataFrame(data)
        return jsonify(df.to_dict(orient='records'))
    else:
        return jsonify({'error': 'Failed to fetch data from ServiceNow. Check the logs for details.'}), 500

if __name__ == '__main__':
    app.run(debug=True)
