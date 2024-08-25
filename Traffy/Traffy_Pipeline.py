from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.email import EmailOperator

from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta
import pytz
import json

from google.oauth2 import service_account
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
    
project_id = 'BKK-Project'
    

# Define Input Path
TRAFFY_RECORDS_API = "https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv"

# Define Output Path
TRAFFY_GCS_BUCKET_PATH = Variable.get("TRAFFY_GCS_BUCKET_PATH")
EMAIL_SUKATAT = Variable.get("EMAIL_SUKATAT")
PATH_TO_GOOGLE_APPLICATION_CREDENTIALS = Variable.get("PATH_TO_GOOGLE_APPLICATION_CREDENTIALS")


default_args = {
    'owner':'Sukatat',
    'email': EMAIL_SUKATAT,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'30 7 * * *'
}

# dag = DAG('Traffy_Data_Pipeline', catchup=False, default_args = default_args)


@task()
def etl_traffy_data(output_path):
    df_traffy_all = pd.read_csv(TRAFFY_RECORDS_API)
    df_traffy_all = df_traffy_all[['ticket_id','timestamp', 'type', 'organization', 'comment', 'photo', 'photo_after',
        'coords', 'address', 'subdistrict', 'district', 'province', 'state', 'star', 'count_reopen', 'last_activity']]
    #df_traffy_all.info()

    # Convert the 'timestamp' column to datetime objects
    df_traffy_all['timestamp'] = pd.to_datetime(
        df_traffy_all['timestamp'],
        errors='coerce',        # Invalid parsing will be set as NaT
        infer_datetime_format=True # This helps to infer the format automatically
    )

    # Remove the microseconds and timezone information
    df_traffy_all['timestamp'] = df_traffy_all['timestamp'].dt.floor('S').dt.tz_localize(None)

    # Ensure the column is in datetime64[us] format
    df_traffy_all['timestamp'] = df_traffy_all['timestamp'].astype('datetime64[us]')


    # Convert the 'last_activity' column to datetime objects
    df_traffy_all['last_activity'] = pd.to_datetime(
        df_traffy_all['last_activity'],
        errors='coerce',        # Invalid parsing will be set as NaT
        infer_datetime_format=True # This helps to infer the format automatically
    )

    # Remove the microseconds and timezone information
    df_traffy_all['last_activity'] = df_traffy_all['last_activity'].dt.floor('S').dt.tz_localize(None)

    # Ensure the column is in datetime64[us] format
    df_traffy_all['last_activity'] = df_traffy_all['last_activity'].astype('datetime64[us]')

    # Set datetime threshold for data
    date_threshold = pd.Timestamp('2022-04-01 00:00:00') # After Ajarn Chadchart
    df_traffy_all = df_traffy_all[df_traffy_all['timestamp'] > date_threshold] # Filter the DataFrame

    # Replace '{' and '}' in the 'type' column
    df_traffy_all['type'] = df_traffy_all['type'].str.replace('{', '', regex=False).str.replace('}', '', regex=False)

    # Drop rows where 'ticket_id' is NaN
    df_traffy_all = df_traffy_all.dropna(subset=['ticket_id', 'type'])

    # Replace ' ' with 'อื่นๆ' in the 'type' column
    df_traffy_all['type'] = df_traffy_all['type'].replace('', 'อื่นๆ')

    print("Data Tranformed Sucessfuly, Continue Loading to GCS")

    print("Start to Authenticate to Google Cloud")
    with open(PATH_TO_GOOGLE_APPLICATION_CREDENTIALS) as source:
        info = json.load(source)

    #client = storage.Client()
    storage_credentials = service_account.Credentials.from_service_account_info(info)
    storage_client = storage.Client(project=project_id, credentials=storage_credentials)
    print("Authenticate to Google Cloud Sucessfully")

    print("Start Loading Data to GCS")
    # Load Data to GCS
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d_%m_%Y")
    filename = f'Traffy_All_Data_{today}.parquet'
    # df_traffy_all.to_parquet(filename, index = False)

    # Convert DataFrame to Parquet format in memory
    table = pa.Table.from_pandas(df_traffy_all, preserve_index=False)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    # Create a bucket object
    bucket = storage_client.bucket('traffy_fondue')
    # Upload the Parquet file to Google Cloud Storage
    blob = bucket.blob(filename)
    blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
    # GOOGLE_APPLICATION_CREDENTIALS = Variable.get("GOOGLE_APPLICATION_CREDENTIALS")
    # client = storage.Client()
    # bucket = client.get_bucket('traffy_fondue')
    print("Data Succesfully Load to GCS")

@task()
def print_success():
    print("SUCCESS: The data is loaded to BigQuery.")

@task()
def print_unsuccess():
    print("UNSUCCESS: The data is NOT loaded to BigQuery.")



@dag(default_args=default_args, start_date=days_ago(1), tags=['Traffy'])
def traffy_pipeline():
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d_%m_%Y")
    
    source_object_filename = f'Traffy_All_Data_{today}.parquet'  # filename for source object to laod to BigQuery
    
    # Create task
    etl_traffy_record_data = etl_traffy_data(output_path=TRAFFY_GCS_BUCKET_PATH)

    # Sensor to check if the file exists in GCS
    check_gcs_file = GCSObjectExistenceSensor(
        task_id='check_gcs_file',
        bucket='traffy_fondue',  # Replace with your bucket name
        object=f'Traffy_All_Data_{today}.parquet',  # Replace with your file path
        mode='poke',  # Use 'poke' mode for simplicity
        timeout=90,  # Timeout after 90 seconds
        poke_interval=30  # Check every 30 seconds
    )

    # Function to decide which path to take
    def choose_branch(**kwargs):
        # Based on some condition, decide which branch to take
        if kwargs['ti'].xcom_pull(task_ids='check_gcs_file'):
            return 'File_Exist_Load_to_BQ'
        else:
            return 'notify_file_not_exists'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch
    )

    File_Exist_Load_to_BQ = GCSToBigQueryOperator(
        task_id='File_Exist_Load_to_BQ',
        bucket='traffy_fondue',
        source_objects=[source_object_filename],
        destination_project_dataset_table="Traffy_Fondue.BKK_records",
        source_format='PARQUET',
        skip_leading_rows=1,
        autodetect = True,
        write_disposition='WRITE_TRUNCATE'
        #dag=dag 
    )

    # Define the EmailOperator for when the file does not exist
    notify_file_not_exists = EmailOperator(
        task_id='notify_file_not_exists',
        to=EMAIL_SUKATAT,
        subject='Traffy Project: File Not Found in GCS',
        html_content=f"""
        <h3>Alert: File Not Found</h3>
        <p>The file <strong>{source_object_filename}Traffy_All_Data_{today}.parquet'</strong> was not found in the GCS bucket <strong>your-bucket-name</strong>.</p>
        <p>Please take the necessary actions.</p>
        """
    )

    Print_Load_Successfully = print_success()

    Print_Load_Unsuccessfully = print_unsuccess()

    # Crate Task Dependency (Create DAG)
    etl_traffy_record_data >> check_gcs_file >> branch_task >> [File_Exist_Load_to_BQ, notify_file_not_exists]
    File_Exist_Load_to_BQ >> Print_Load_Successfully 
    notify_file_not_exists >> Print_Load_Unsuccessfully

Traffy_DAG = traffy_pipeline()