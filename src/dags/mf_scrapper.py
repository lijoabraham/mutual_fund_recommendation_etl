import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


args = {
    'start_date': datetime(2022, 12, 30,
        tzinfo=pendulum.timezone("Asia/Kolkata")),
    'email': ['lijoabraham1234@gmail.com'],
    'email_on_failure': True
}

schedule_interval = '30 00 * * *'

dag = DAG(
    dag_id='mf_scrapper',
    default_args=args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    catchup=False,
    tags=['mf_scrapper']
)

scrap_data = BashOperator(
    task_id='mf_scrapper',
    bash_command='python /usr/local/spark/app/jobs/mfscrapper.py',
    dag=dag,
)

# Spark-Submit cmd: spark-submit --master spark://spark:7077 --files app/configs/scrapper.json --py-files app/packages.zip --name arrow-spark --verbose --queue root.default app/etl_job.py

spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/spark/app/jobs/etl_job.py",
        conn_id="spark_default",
        verbose=1,
        py_files="/usr/local/spark/app/packages.zip",
        files="/usr/local/spark/app/configs/scrapper.json",
        dag=dag
    )


scrap_data >> spark_job