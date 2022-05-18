from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2


def get_Redshift_connection():
    # Ariflow connections 에서 받음
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def extract(**context):
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    part = context["params"]["part"]
    API_key = context["params"]["API_key"]

    task_instance = context['task_instance']
    execution_date = context['execution_date']

    link = f'https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API_key}'

    logging.info(execution_date)
    f = requests.get(link)
    return (f.json()['daily'])


def transform(**context):
    # 앞 extract task에서 읽어온 데이터를 xcom을 이용해서 가져옴
    daily_weather = context["task_instance"].xcom_pull(
        key="return_value", task_ids="extract")
    daily_weather_info = []
    for drow in daily_weather:
        date = datetime.fromtimestamp(drow["dt"]).strftime(
            '%Y-%m-%d')  # dt parsing 필요
        temp = drow['temp']
        daily_weather_info.append(
            {'date': date, 'temp': temp['day'], 'min_temp': temp['min'], 'max_temp': temp['max']})
    return daily_weather_info


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    daily_weather_info = context["task_instance"].xcom_pull(
        key="return_value", task_ids="transform")
    # 20220518: feedback
    # sql = f"""CREATE TABLE {schema}.temp_{table}
    #          AS SELECT * FROM {schema}.{table};"""
    # for drow in daily_weather_info:
    #     if drow != "":
    #         sql += f"""INSERT INTO {schema}.temp_{table} (date, temp, min_temp, max_temp) VALUES ('{drow['date']}', '{drow['temp']}', '{drow['min_temp']}', '{drow['max_temp']}');"""

    # sql += "BEGIN; DELETE FROM {schema}.{table};".format(
    #     schema=schema, table=table)
    # sql += f"""INSERT INTO {schema}.{table}
    #            SELECT date, temp, min_temp, max_temp, created_date
    #             FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq FROM {schema}.temp_{table}) WHERE seq = 1; """
    # sql += """END;"""

    # euni: create_date를 살리기 위해
    # redshift에서: CREATE TABLE AS(CTAS) 테이블은 자신이 생성된 테이블로부터 제약 조건, 자격 증명 열, 기본 열 값 또는 기본 키를 상속하지 않습니다.
    # row_number() 잘못이해함..
    sql = "BEGIN;"
    for drow in daily_weather_info:
        if drow != "":
            sql += f"""INSERT INTO {schema}.{table} (date, temp, min_temp, max_temp) VALUES ('{drow['date']}', '{drow['temp']}', '{drow['min_temp']}', '{drow['max_temp']}');"""

    sql += f"""CREATE TABLE {schema}.temp_{table} AS SELECT date, temp, min_temp, max_temp, created_date 
                FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq FROM {schema}.{table}) WHERE seq = 1;"""
    sql += f"DELETE FROM {schema}.{table};"
    sql += f"""INSERT INTO {schema}.{table} SELECT * FROM {schema}.temp_{table};"""
    sql += f"""DROP TABLE IF EXISTS {schema}.temp_{table};"""
    sql += """END;"""
    logging.info(sql)
    cur.execute(sql)


dag_second_assignment = DAG(
    dag_id='daily_weather',
    start_date=datetime(2022, 5, 13),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 1 * * *',  # 적당히 조절
    max_active_runs=1,  # 한번에 실행될 수 있는 DAG 숫자, DAG에만 적용됨 default_args전에 넣어줘야함
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)

# task 3개로 나눔 (ETL)
extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'lat': 37.5665,
        'lon': 126.978,
        'part': 'current,minutely,hourly',
        # Airflow Variable에서 여기서 받아옴
        'API_key': Variable.get("weather_api_key")
    },
    provide_context=True,
    dag=dag_second_assignment)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    params={
    },
    provide_context=True,
    dag=dag_second_assignment)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'jaxgxxnxee',  # 스키마 파라미터로 받기
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag=dag_second_assignment)

extract >> transform >> load
