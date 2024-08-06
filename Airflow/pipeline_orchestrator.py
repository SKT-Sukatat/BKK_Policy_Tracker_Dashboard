from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

TOP_30_POLICIES_URL = 'https://policy.bangkok.go.th/tracking/frontend/web/index.php?r=site%2Findex'

# Define output path
# top30_output_path =


default_args = {
    'owner':'Sukatat'
}

dag = DAG('test', catchup=False, default_args = default_args)

# 
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
    df_rushing_policy.to_parquet(output_path, index=False)

# t1 = PythonOperator()

# t2 - PythonOperator()

# t1 >> t2