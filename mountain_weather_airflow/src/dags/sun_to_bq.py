from datetime import datetime, timedelta

from airflow.decorators import dag, task
from google.oauth2 import service_account
from scrape_sun_data import find_sun_data

default_args = {
    'owner': 'PMW',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    default_args=default_args,
    dag_id='sun_to_bq_v03',
    description=
    'Scrape sunrise and sunset data from timeanddate.com and send to BQ.',
    start_date=datetime(2024, 7, 26),
    schedule_interval='@daily'
)
def sun_to_bq():
    @task()
    def scrape(csv_name):
        return find_sun_data(csv_name)

    @task()
    def load_df_to_bq(df):
        creds = service_account.Credentials.from_service_account_file(
            'keys/mountain-weather-data-f29d5a51ba66.json'
        )
        df.to_gbq(
            project_id='mountain-weather-data',
            destination_table='from_airflow.sun_staging',
            if_exists='append',
            credentials=creds
        )

    load_df_to_bq(scrape('config/mtns_for_timeanddate.csv'))


dag = sun_to_bq()
