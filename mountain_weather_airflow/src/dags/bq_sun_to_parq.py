from datetime import datetime, timedelta

from airflow.decorators import dag, task
from google.oauth2 import service_account
import pandas_gbq


default_args = {
    'owner': 'PMW',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    default_args=default_args,
    dag_id='bq_sun_to_parq_v01',
    description=
    'Save sunrise/set staging data from BQ as a backup local parquet file.',
    start_date=datetime(2024, 7, 26),
    schedule_interval='@daily'
)
def bq_sun_to_parq():
    @task()
    def save_parq_sun_df_from_bq(backup_name):
        creds = service_account.Credentials.from_service_account_file(
            'keys/mountain-weather-data-f29d5a51ba66.json'
        )
        pandas_gbq.context.credentials = creds
        pandas_gbq.context.project = 'mountain-weather-data'

        df = pandas_gbq.read_gbq(
            'SELECT * FROM `mountain-weather-data.from_airflow.sun_staging`',
            project_id='mountain-weather-data'
        )
        df.to_parquet(backup_name)

    save_parq_sun_df_from_bq('backups/airflow_sun_staging.parquet')


dag = bq_sun_to_parq()
