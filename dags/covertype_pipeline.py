"""
DAG del Proyecto 1 - MLOps.
Cada ejecucion hace UNA peticion a la API externa y cumple el ciclo completo:
recolectar -> procesar -> preparar -> entrenar.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import covertype_lib as lib

default_args = {
    "owner": "daniel-nino",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="covertype_pipeline",
    description="Recolecta un batch de la API, procesa, prepara y entrena",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["proyecto1", "mlops", "covertype"],
) as dag:

    t1 = PythonOperator(task_id="recolectar", python_callable=lib.recolectar)
    t2 = PythonOperator(task_id="procesar", python_callable=lib.procesar)
    t3 = PythonOperator(task_id="preparar", python_callable=lib.preparar)
    t4 = PythonOperator(task_id="entrenar", python_callable=lib.entrenar)

    t1 >> t2 >> t3 >> t4
