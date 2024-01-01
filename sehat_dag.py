# Import Library Airflow ===

from airflow import DAG
from airflow.operators.python import PythonOperator
from sehat_script import *

#==============================================================

# Import Library Tambahan ===

from datetime import datetime

#==============================================================

# DAG workflow

with DAG (
    dag_id='sehat_dag',
    description='Menghitung Gross Profit berdasarkan data Purchase dan Sales',
    schedule='@daily',
    start_date=datetime(2023,1,1),
    tags=['sehat'],
    catchup=False
) as dag:
    
    taskStart = PythonOperator(
        task_id = "Start",
        python_callable = start,    
    )
    
    taskCountCOGS = PythonOperator(
        task_id = "Count_COGS_on_BI",
        python_callable = countCOGS,    
    )
    
    taskCountCOGAS_BI = PythonOperator(
        task_id = "Count_COGAS_on_BI",
        python_callable = countCOGAS_BI,    
    )
    
    taskCountCOGAS_purchase = PythonOperator(
        task_id = "Count_COGAS_on_Purchases",
        python_callable = countCOGAS_purchase,    
    )
    
    taskCountNetSales = PythonOperator(
        task_id = "Count_Net_Sales",
        python_callable = countNS,    
    )
    
    taskCountGrossProfit = PythonOperator(
        task_id = "Count_Gross_Profit",
        python_callable = countGP,    
    )
    
    taskCountEI = PythonOperator(
        task_id = "Count_Ending_Inventory",
        python_callable = countEI,    
    )
    
    taskFinish = PythonOperator(
        task_id = "Finish",
        python_callable = finish,
    )
   
    
    taskStart >> taskCountCOGS >> taskCountCOGAS_BI >> taskCountCOGAS_purchase >> taskCountEI >> taskCountNetSales >> taskCountGrossProfit >> taskFinish

#==============================================================