from airflow.models import DAG, Variable
from airflow.decorators import dag, task
from airflow.operators.python import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import os
from datetime import datetime
import pytz
import re


# Define Input Path
TOP_30_POLICIES_URL = Variable.get("TOP_30_POLICIES_URL")

# Define Output Path
PROGRESS_OF_POLICY_BUCKET_OUTPUT = Variable.get("PROGRESS_OF_POLICY_BUCKET_OUTPUT")
TOP_30_POLICY_BUCKET_PATH = Variable.get("TOP_30_POLICY_BUCKET_PATH")
EMAIL_SUKATAT = Variable.get("EMAIL_SUKATAT")


default_args = {
    'owner':'Sukatat',
    'email': EMAIL_SUKATAT,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'30 8 * * *'
}

dag = DAG('BKK_Policy', catchup=False, default_args = default_args)


@task()
def et_top_30_policy(output_path):
    # Read HTML tables into a list of DataFrame objects.
    all_tables = pd.read_html(TOP_30_POLICIES_URL)

    # Define Pandas Dataframe
    df_rushing_policy = all_tables[0]

    # Check number of table(s)
    df_rushing_policy.dropna(axis = 0, inplace = True)

    new_name = {'เป้าหมายตามนโยบาย ผว.กทม.': 'Goal', 'หน่วยนับ':'Unit', 'เป้าหมายรวม': 'Total_Goals',
        'เป้าหมายจากสำนัก':'Goal_Departments', 'เป้าหมายจาก 50 เขต':'Goal_50_Districts', 'ผลการดำเนินงาน*':'Total_Progress_in_Unit',
        'ความคืบหน้า* ของ KPI (%)':'Progress_in_Percent'}

    df_rushing_policy.rename(columns = new_name, inplace = True)

    # new data frame with split value columns
    splited_goal = df_rushing_policy["Goal"].str.split(":", n=1, expand=True)

    # making separate first name column from new data frame
    df_rushing_policy["Goal"] = splited_goal[0]

    # making separate last name column from new data frame
    df_rushing_policy["Related_OKR"] = splited_goal[1]

    # Dropping old Name columns
    # df_rushing_policy.drop(columns=["Goal"], inplace=True)

    df_rushing_policy['Related_OKR'] = df_rushing_policy['Related_OKR'].str.replace('OKR ', '')
    df_rushing_policy['Related_OKR'] = df_rushing_policy['Related_OKR'].str.replace('(** ค่าเฉลี่ย **)', '(Mean Value)')

    splited_goal_2 = df_rushing_policy["Goal"].str.split(" ", n=1, expand=True)
    df_rushing_policy["ID_Result"] = splited_goal_2[0]
    df_rushing_policy["Goal"] = splited_goal_2[1]

    df_rushing_policy['ID_Result'] = df_rushing_policy['ID_Result'].str.strip('.')

    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d-%m-%Y")
    df_rushing_policy['Updated_Date'] = today

    df_rushing_policy['Updated_Date'] = df_rushing_policy['Updated_Date'].astype('datetime64[us]')
    df_rushing_policy = df_rushing_policy.astype({'ID_Result': float})

    df_rushing_policy = df_rushing_policy[['Updated_Date','ID_Result', 'Related_OKR', 'Goal', 'Unit', 'Total_Goals', 'Goal_Departments',
        'Goal_50_Districts', 'Total_Progress_in_Unit', 'Progress_in_Percent']]

    # Load Data to GCS
    top30_output_path = output_path + '/top-policy-' + str(today) + ".parquet"
    df_rushing_policy.to_parquet(top30_output_path, index=False)
    print(f"Rushing Policy Output to {top30_output_path}")


@task()
def et_all_policy(output_path):
    # Declare Blank DataFrame
    df_progress = pd.DataFrame()

    # Access and concat all DataFrames to same DataFrames
    for i in range(1,7):
        policy_url = "https://policy.bangkok.go.th/tracking/frontend/web/index.php?r=site%2Findexcheckreportkpi2&page=" + str(i) +"&per-page=50"
        progress_table = pd.read_html(policy_url)
        df_progress = pd.concat([df_progress, progress_table[0]], ignore_index=True, axis = 0)

    ## Rename Columns
    new_col_names = {'KEY_RESULT':'Goal', 'ค่าเป้าหมาย/ปี':'Yearly_Goal', 'ผลดำเนินงาน (รวม)':'Total_Progress_in_Unit', 'หน่วยนับ':'Unit',
        'ตค.66':'Oct_23', 'พย.66':'Nov_23', 'ธค.66':'Dec_23', 'มค.67':'Jan_24', 'กพ.67':'Feb_24', 'มี.ค.67':'Mar_24', 'เม.ย.67':'Apr_24',
        'พค.67':'May_24', 'มิ.ย.67':'Jun_24', 'กค.67':'July_24', 'สค.67':'Aug_24', 'กย.67':'Sept_24'}
    df_progress.rename(columns = new_col_names, inplace = True)

    # Drop Null Value
    df_progress.dropna(axis = 0, inplace = True)

    # Reset Index
    df_progress = df_progress.reset_index()
    df_progress.drop('index', axis = 1, inplace = True)

    # df_progress.rename(columns = {'#':'Number'}, inplace = True)

    # Set Index
    df_progress['Number'] = df_progress['#'].astype('int')
    df_progress['#'] = df_progress['#'].astype('int')
    df_progress.set_index(df_progress['#'], inplace = True)
    df_progress.drop('#', axis = 1, inplace = True)

    # Function to extract all occurrences of "OKR \d+\.\d+\.\d+" (OKR 0.0.0)
    def extract_okr_numbers(text):
        okrs = re.findall(r'OKR \d+\.\d+\.\d+(?:\.\d+)?', text)
        okrs = ''.join(okrs)
        result = okrs.replace('OKR ', '', )
        return result

    def remove_okr_numbers(text):
        return re.sub(r'OKR \d+\.\d+\.\d+(?:\.\d+)?', '', text).strip()

    # Apply the function to the Goal column
    df_progress['Related_OKRs'] = df_progress['Goal'].apply(extract_okr_numbers)
    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_okr_numbers(x))


    # Replace all Blank value by NaN (Null)
    df_progress['Related_OKRs'] = df_progress['Related_OKRs'].replace('', np.nan)

    # Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
    def extract_kpi_numbers(text):
        kpi = re.findall(r'KPI \d+(?:\.\d+){2,3}', text)
        kpi = ''.join(kpi)
        result = kpi.replace('KPI ', '', )
        return result

    def remove_kpi_numbers(text):
        return re.sub(r'KPI \d+(?:\.\d+){2,3}', '', text).strip()

    df_progress['Related_KPI'] = df_progress['Goal'].apply(extract_kpi_numbers)
    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_kpi_numbers(x))

    df_progress['Related_KPI'] = df_progress['Related_KPI'].replace('', np.nan)

    # Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
    def extract_percent_progress(text):
        progress_percent = re.findall(r'\d{1,3}(?:,\d{3})*\.\d+%', text)
        return ''.join(progress_percent)

    def remove_percent_progress(text):
        return re.sub(r'\d{1,3}(?:,\d{3})*\.\d+%', '', text).strip()


    # Dealing with Total_Progress_in_Percent
    df_progress['Total_Progress_in_Percent'] = df_progress['Goal'].apply(extract_percent_progress)
    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_percent_progress(x))

    df_progress['Goal'] = df_progress['Goal'].str.replace(':', '')

    def extract_goal_id(text):
        goal_id = re.findall(r'\d+\.\d?', str(text))
        result = ''.join(goal_id)
        return result
    # def extract_patterns(text):
    #     return re.findall(r'\b\d+\.\d*\b', text)

    def remove_goal_id(text):
        return re.sub(r'\d+\.\d?', '', text).strip()

    df_progress['ID_Result'] = df_progress['Goal'].apply(extract_goal_id)
    df_progress['ID_Result'] = df_progress['ID_Result'].replace('', np.nan)
    df_progress['ID_Result'] = df_progress['ID_Result'].str.strip('.')

    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_goal_id(x))

    # Create Updated_Date columns to store the date
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d-%m-%Y")
    df_progress['Updated_Date'] = today

    # Convert to datetime64[us] datatype
    df_progress['Updated_Date'] = df_progress['Updated_Date'].astype('datetime64[us]')
    df_progress = df_progress.astype({'ID_Result': float})

    df_progress = df_progress[['Updated_Date','Number','Goal','ID_Result','Unit', 'Related_OKRs', 'Related_KPI', 'Yearly_Goal', 'Total_Progress_in_Unit', 'Total_Progress_in_Percent',
                'Oct_23', 'Nov_23', 'Dec_23', 'Jan_24', 'Feb_24', 'Mar_24', 'Apr_24', 'May_24', 'Jun_24', 'July_24', 'Aug_24', 'Sept_24']]

    # Load Data to GCS
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d-%m-%Y")
    all_policy_output_path = output_path + '/All_Policy_Month_Progress/all-policy-' + str(today) + ".parquet"
    df_progress.to_parquet(all_policy_output_path, index=False)
    print(f"All policy Output to {all_policy_output_path}")


@task()
def merge_data(top_30_policy_path, all_policy_path, joined_output_path):
    # Read the data in parquet format from GCS # TOP_30_POLICY_BUCKET_PATH
    df_rushing_policy_for_join = pd.read_parquet(top_30_policy_path + '/top-policy-' + str(today) + ".parquet", columns=['ID_Result','Goal'])
    df_progress_for_join = pd.read_parquet(all_policy_path + '/All_Policy_Month_Progress/all-policy-' + str(today) + ".parquet", columns = ['ID_Result', 'Yearly_Goal', 'Total_Progress_in_Unit', 'Unit', 'Total_Progress_in_Percent',
                                                                            'Oct_23','Nov_23', 'Dec_23', 'Jan_24', 'Feb_24', 'Mar_24', 'Apr_24', 'May_24', 'Jun_24','July_24', 'Aug_24', 'Sept_24'])

    # Join the DataFrames by merge function
    df_joined = df_rushing_policy_for_join.merge(df_progress_for_join, left_on = 'ID_Result', right_on = 'ID_Result', how = 'left')

    # Create Updated_Date columns to store the date
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d-%m-%Y")
    df_joined['Updated_Date'] = today

    all_months = ['Oct_23', 'Nov_23', 'Dec_23', 'Jan_24', 'Feb_24', 'Mar_24', 'Apr_24', 'May_24', 'Jun_24', 'July_24', 'Aug_24', 'Sept_24']

    for month in all_months:
        df_joined[month + '_percent'] = df_joined[month].div(df_joined.Yearly_Goal, axis=0)*100
        df_joined[month + '_percent'] = df_joined[month + '_percent'].round(2)

    # Change order of columns
    df_joined = df_joined[['ID_Result', 'Updated_Date', 'Goal', 'Yearly_Goal', 'Total_Progress_in_Unit', 'Unit',
                           'Total_Progress_in_Percent', 'Oct_23', 'Nov_23', 'Dec_23', 'Jan_24','Feb_24', 'Mar_24', 'Apr_24', 'May_24',
                           'Jun_24', 'July_24', 'Aug_24','Sept_24',  'Oct_23_Percent', 'Nov_23_Percent',
                           'Dec_23_Percent', 'Jan_24_Percent', 'Feb_24_Percent', 'Mar_24_Percent',
                           'Apr_24_Percent', 'May_24_Percent', 'Jun_24_Percent', 'July_24_Percent',
                           'Aug_24_Percent', 'Sept_24_Percent']]

    # Load Data to GCS
    joined_output_path = joined_output_path + '/Top_30_Policy_Month_Progress/top-policy-with-month-progress-' + str(today) + ".parquet"
    df_joined.to_parquet(joined_output_path, index=False)
    print(f"Top 30 policy with Month Output to {joined_output_path}")


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=['workshop'])
def bkk_policy_pipeline():
    today = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%d-%m-%Y")
    
    # Create task
    t1 = et_top_30_policy(TOP_30_POLICY_BUCKET_PATH)
    t2 = GCSToBigQueryOperator(
        task_id='Load Top 30 Policy',
        bucket='bkk-policy-data',
        source_objects=['gs://bkk-policy-data/Progress_of_Policy/All_Policy_Month_Progress/' + "all-policy-" + str(today) + ".parquet"],
        destination_project_dataset_table="Progress_of_Policy.All_Policy",
        skip_leading_rows=1,
        autodetect = True,
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )
    t3 = et_all_policy(PROGRESS_OF_POLICY_BUCKET_OUTPUT)
    t4 = merge_data(TOP_30_POLICY_BUCKET_PATH, PROGRESS_OF_POLICY_BUCKET_OUTPUT, PROGRESS_OF_POLICY_BUCKET_OUTPUT)
    t5 = GCSToBigQueryOperator(
        task_id='Load Top 30 Policy with Progress',
        bucket='bkk-policy-data',
        source_objects=['gs://bkk-policy-data/Progress_of_Policy/Top_30_Policy_Month_Progress/top-policy-with-month-progress-' + str(today) + ".parquet"],
        destination_project_dataset_table="Progress_of_Policy.Top_30_Policy",
        skip_leading_rows=1,
        autodetect = True,
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )

    # Crate Task Dependency (Create DAG)
    t1 >> t2
    [t2, t3] >> t4
    t4 >> t5

bkk_policy_pipeline()