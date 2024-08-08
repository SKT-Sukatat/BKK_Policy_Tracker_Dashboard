from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
from datetime import date
import re

# Define Input Path
TOP_30_POLICIES_URL = 'https://policy.bangkok.go.th/tracking/frontend/web/index.php?r=site%2Findex'

# Define Output Path
TOP_30_BUCKET_OUTPUT = os.getenv('top_policy_bukcet_path')
ALL_POLICY_TRANFORMED_BUCKET_PATH = os.getenv('top_30_policy_transformed_path')


default_args = {
    'owner':'Sukatat'
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

    new_name = {'เป้าหมายตามนโยบาย ผว.กทม.': 'Goal', 'หน่วยนับ':'Unit', 'เป้าหมายรวม': 'Total Goals',
        'เป้าหมายจากสำนัก':'Goal (Departments)', 'เป้าหมายจาก 50 เขต':'Goal (50 Districts)', 'ผลการดำเนินงาน*':'Total Progress (Unit)',
        'ความคืบหน้า* ของ KPI (%)':'Progress (%)'}

    df_rushing_policy.rename(columns = new_name, inplace = True)

    # new data frame with split value columns
    splited_goal = df_rushing_policy["Goal"].str.split(":", n=1, expand=True)

    # making separate first name column from new data frame
    df_rushing_policy["Goal"] = splited_goal[0]

    # making separate last name column from new data frame
    df_rushing_policy["Related OKR"] = splited_goal[1]

    # Dropping old Name columns
    # df_rushing_policy.drop(columns=["Goal"], inplace=True)

    df_rushing_policy['Related OKR'] = df_rushing_policy['Related OKR'].str.replace('OKR ', '')
    df_rushing_policy['Related OKR'] = df_rushing_policy['Related OKR'].str.replace('(** ค่าเฉลี่ย **)', '(Mean Value)')

    splited_goal_2 = df_rushing_policy["Goal"].str.split(" ", n=1, expand=True)
    df_rushing_policy["No. (Goal)"] = splited_goal_2[0]
    df_rushing_policy["Goal"] = splited_goal_2[1]

    df_rushing_policy = df_rushing_policy[['No. (Goal)', 'Related OKR', 'Goal', 'Unit', 'Total Goals', 'Goal (Departments)',
        'Goal (50 Districts)', 'Total Progress (Unit)', 'Progress (%)']]

    df_rushing_policy['No. (Goal)'] = df_rushing_policy['No. (Goal)'].str.strip('.')

    # Load Data to GCS
    today = date.today().strftime("%d-%m-%Y")
    top30_output_path = output_path + '/top-policy-' + str(today) + ".parquet"
    df_rushing_policy.to_parquet(top30_output_path, index=False)


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
    new_col_names = {'KEY_RESULT':'Goal', 'ค่าเป้าหมาย/ปี':'Yearly Goal', 'ผลดำเนินงาน (รวม)':'Total Progress (Unit)', 'หน่วยนับ':'Unit',
        'ตค.66':'Oct 23', 'พย.66':'Nov 23', 'ธค.66':'Dec 23', 'มค.67':'Jan 24', 'กพ.67':'Feb 24', 'มี.ค.67':'Mar 24', 'เม.ย.67':'Apr 24',
        'พค.67':'May 24', 'มิ.ย.67':'Jun 24', 'กค.67':'July 24', 'สค.67':'Aug 24', 'กย.67':'Sept 24'}
    df_progress.rename(columns = new_col_names, inplace = True)

    # Drop Null Value
    df_progress.dropna(axis = 0, inplace = True)

    # Reset Index
    df_progress = df_progress.reset_index()
    df_progress.drop('index', axis = 1, inplace = True)

    # Set Index
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
    df_progress['Related OKRs'] = df_progress['Goal'].apply(extract_okr_numbers)
    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_okr_numbers(x))


    # Replace all Blank value by NaN (Null)
    df_progress['Related OKRs'] = df_progress['Related OKRs'].replace('', np.nan)

    # Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
    def extract_kpi_numbers(text):
        kpi = re.findall(r'KPI \d+(?:\.\d+){2,3}', text)
        kpi = ''.join(kpi)
        result = kpi.replace('KPI ', '', )
        return result

    def remove_kpi_numbers(text):
        return re.sub(r'KPI \d+(?:\.\d+){2,3}', '', text).strip()

    df_progress['Related KPI'] = df_progress['Goal'].apply(extract_kpi_numbers)
    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_kpi_numbers(x))

    df_progress['Related KPI'] = df_progress['Related KPI'].replace('', np.nan)

    # Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
    def extract_percent_progress(text):
        progress_percent = re.findall(r'\d{1,3}(?:,\d{3})*\.\d+%', text)
        return ''.join(progress_percent)

    def remove_percent_progress(text):
        return re.sub(r'\d{1,3}(?:,\d{3})*\.\d+%', '', text).strip()


    # Dealing with Total Progress (%)
    df_progress['Total Progress (%)'] = df_progress['Goal'].apply(extract_percent_progress)
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


    df_progress['No. (Goal)'] = df_progress['Goal'].apply(extract_goal_id)
    df_progress['No. (Goal)'] = df_progress['No. (Goal)'].replace('', np.nan)
    df_progress['No. (Goal)'] = df_progress['No. (Goal)'].str.strip('.')

    df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_goal_id(x))

    df_progress = df_progress[['Goal','No. (Goal)','Unit', 'Related OKRs', 'Related KPI', 'Yearly Goal', 'Total Progress (Unit)', 'Total Progress (%)', 
                'Oct 23', 'Nov 23', 'Dec 23', 'Jan 24', 'Feb 24', 'Mar 24', 'Apr 24', 'May 24', 'Jun 24', 'July 24', 'Aug 24', 'Sept 24']]


    # Load Data to GCS
    today = date.today().strftime("%d-%m-%Y")
    all_policy_output_path = output_path + '/all-policy-' + str(today) + ".parquet"
    df_progress.to_parquet(all_policy_output_path, index=False)
    

# t1 = PythonOperator()

# t2 - PythonOperator()

@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=['workshop'])
def bkk_policy_pipeline():
    """
    # Exercise4: Final DAG
    ใน exercise นี้จะนำโค้ดที่เคยเขียนไว้ใน workshop1 มาทำให้เป็น pipeline บน Airflow [ทบทวนได้ที่นี่](https://colab.research.google.com/drive/1LQDVS0ayxFKF_ln-mc4CqLeayxzUKqZP?authuser=1)
    """
    
    # TODO: สร้าง task จาก function ด้านบน และใส่ parameter ให้ถูกต้อง
    today = date.today().strftime("%d-%m-%Y")
    
    t1 = et_top_30_policy(TOP_30_BUCKET_OUTPUT)
    t2 = et_all_policy(ALL_POLICY_TRANFORMED_BUCKET_PATH)
    t3 = merge_data(TOP_30_BUCKET_OUTPUT + '/top-policy-' + str(today) + ".parquet", ALL_POLICY_TRANFORMED_BUCKET_PATH + '/all-policy-' + str(today) + ".parquet", #final_output_path)

    # TODO: สร้าง dependency ให้ถูกต้อง (ต้องรัน task 3 หลังจาก 1 และ 3 เสร็จเท่านั้น)
    [t1, t2] >> t3

workshop4_pipeline()
