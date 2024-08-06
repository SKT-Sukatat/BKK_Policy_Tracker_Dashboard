from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests


MYSQL_CONNECTION = "mysql_default"   # ชื่อของ connection ใน Airflow ที่เซ็ตเอาไว้
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"

# ตัวแปรของ output_path ที่จะเซฟ
mysql_output_path = "/home/airflow/gcs/data/transaction_data_merged.parquet"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.parquet"
final_output_path = "/home/airflow/gcs/data/workshop4_output.parquet"

default_args = {
    'owner': 'datath',
}


# TODO: สร้าง task แรก เพื่อ Query ข้อมูลจาก MySQL
@task()
def get_data_from_mysql(output_path):
    # รับ output_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    product = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.product")
    customer = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.customer")
    transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.transaction")

    # Merge data จาก 2 DataFrame เหมือนใน workshop1
    merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
    
    # Save ไฟล์ parquet ไปที่ output_path ที่รับเข้ามา
    # จะไปอยู่ที่ GCS โดยอัตโนมัติ
    merged_transaction.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")


# TODO: สร้าง task ที่ 2 (ทำงานพร้อม task แรก) เพื่อเรียกข้อมูล conversion rate
@task()
def get_conversion_rate(output_path):
    # ส่ง request ไป get ข้อมูลจาก CONVERSION_RATE_URL
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.drop(columns=['id'])

    # แปลง column ให้เป็น date แล้วเซฟไฟล์ parquet
    df['date'] = pd.to_datetime(df['date'])
    df.to_parquet(output_path, index=False)
    # Print to show successful task
    print(f"Output to {output_path}")


# TODO: สร้าง task ที่ 3 เพื่อ merge data (ต้องทำงานหลัง task 1 และ 2 เท่านั้น)
@task()
def merge_data(transaction_path, conversion_rate_path, output_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    transaction = pd.read_parquet(transaction_path)
    conversion_rate = pd.read_parquet(conversion_rate_path)

    # merge 2 DataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")
    
    # แปลงราคา ให้เป็น total_amount และ thb_amount
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]

    # drop column ที่ไม่ใช้ และเปลี่ยนชื่อ column
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)

    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
        'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']

    # save ไฟล์ Parquet
    final_df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")
    print("== End of Workshop 4 ʕ•́ᴥ•̀ʔっ♡ ==")


# TODO: สร้าง dag
@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=['workshop'])
def workshop4_pipeline():
    """
    # Exercise4: Final DAG
    ใน exercise นี้จะนำโค้ดที่เคยเขียนไว้ใน workshop1 มาทำให้เป็น pipeline บน Airflow [ทบทวนได้ที่นี่](https://colab.research.google.com/drive/1LQDVS0ayxFKF_ln-mc4CqLeayxzUKqZP?authuser=1)
    """
    
    # TODO: สร้าง task จาก function ด้านบน และใส่ parameter ให้ถูกต้อง
    t1 = get_data_from_mysql(mysql_output_path)
    t2 = get_conversion_rate(conversion_rate_output_path)
    t3 = merge_data(mysql_output_path, conversion_rate_output_path, final_output_path)

    # TODO: สร้าง dependency ให้ถูกต้อง (ต้องรัน task 3 หลังจาก 1 และ 3 เสร็จเท่านั้น)
    [t1, t2] >> t3

workshop4_pipeline()
