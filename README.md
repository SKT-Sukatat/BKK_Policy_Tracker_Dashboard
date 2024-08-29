# BKK Problem and Policy Tracking System

## Introduction
BKK Problem and Policy Tracking System is a system of data pipelines and dashboards that enable users to monitor and understand the current state of urban issues reported on the Traffy Fondue platform. Additionally, it tracks the progress of policies implemented by the Bangkok Metropolitan Administration (BMA). For example, users can monitor trends in various problem types in Bangkok, such as footpaths, roads, electricity, and garbage, and assess the impact of Traffy Fondue promotions by tracking the number of cases reported.


## Overall Architecture
![BKK_Policy_3](https://github.com/user-attachments/assets/cbbddd52-a8ff-4ca7-9c9d-5ed355105d8c)
This diagram show the overall of data pipeline and connection to Looker Studio, as a dashboard.

## Technology Used
1. **Programming Language** - Python
2. **Data Transformation Library** - Pandas, PySpark
3. **Data Lake** - Google Cloud Storage
4. **Data Warehouse** - BigQuery
5. **Pipeline Orchestration** - Airflow
6. **Data Visualization Tools** - Google Looker Studio
7. **Host server** - Linux VPS


## Pipelines for Traffy Fondue Data
**Airflow Directed Acyclic Graphs (DAG)**

![DAG](https://github.com/user-attachments/assets/2fcca232-2a93-4a21-b58b-c60e065d3c78)

- Set up in [Traffy_Pipeline.py](https://github.com/SKT-Sukatat/BKK_Problem_and_Policy_Tracking_System/blob/main/Traffy_Pipeline.py)

## Dashboard
![Link Looker Studio Dashboard](https://lookerstudio.google.com/reporting/e016ed89-b0c6-46cc-a457-0aab0c94cfff)

![chart_2](https://github.com/user-attachments/assets/4f04fe5e-c3e2-4452-80df-56593526553a)
This picture illustrates a dashboard example displaying the number of problem cases for each type, the count of reviews by star rating, and a line chart showing the number of problem cases for each type over time.
