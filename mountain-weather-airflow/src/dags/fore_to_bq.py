from datetime import datetime, timedelta

from airflow.decorators import dag, task
from google.oauth2 import service_account
from scrape_mountain_weather_data import scrape_weather

default_args = {
    'owner': 'PMW',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    default_args=default_args,
    dag_id='fore_to_bq_v03',
    description=
    'Scrape forecast data from mountain-forecast.com and send to BQ.',
    start_date=datetime(2024, 7, 26),
    schedule_interval='@daily'
)
def fore_to_bq():
    @task()
    def scrape(json_name):
        return scrape_weather(json_name)

    @task()
    def load_df_to_bq(df):
        creds = service_account.Credentials.from_service_account_file(
            'keys/mountain-weather-data-f29d5a51ba66.json'
        )
        df.to_gbq(
            project_id='mountain-weather-data',
            destination_table='from_airflow.forecast_staging',
            if_exists='append',
            credentials=creds
        )

    load_df_to_bq(scrape('config/mtns_elevs_for_scraping.json'))


dag = fore_to_bq()
