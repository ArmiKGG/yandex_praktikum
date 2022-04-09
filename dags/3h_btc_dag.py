from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pymongo


def make_requests(**context):
    ti = context['ti']
    url = 'https://api.exchangerate.host/latest'
    response = requests.get(url, params={'base': 'BTC'})
    data = response.json()
    print('---------------------------------------------------')
    print('API Response')
    print(data, flush=True)
    print('---------------------------------------------------')
    ti.xcom_push(key='btc_usd', value=data)


def handle_data(**context):
    ti = context['ti']
    btc_usd = ti.xcom_pull(key='btc_usd', task_ids='api_request')
    btc_usd_ts = {
        'pair': 'BTC/USD',
        'closed': None,
        'ts': datetime.utcnow().timestamp()
    }
    if btc_usd['success']:
        btc_usd_ts['closed'] = btc_usd['rates']['USD']

    print('---------------------------------------------------')
    print('Data transformed')
    print(btc_usd_ts, flush=True)
    print('---------------------------------------------------')
    # this xcom_push is unnecessary because this is our final task
    ti.xcom_push(key='btc_usd', value=btc_usd_ts)


def load_data(**context):
    client = pymongo.MongoClient('mongodb+srv://arman:arman@cluster0.qmeif.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    data_store = client.yandex_praktikum.btc_usd
    ti = context['ti']
    btc_usd = ti.xcom_pull(key='btc_usd', task_ids='transform')
    data_store.insert_one(btc_usd)
    print('---------------------------------------------------')
    print('Data pushed to mongo - to take a look at all data')
    print(btc_usd, flush=True)
    print('---------------------------------------------------')
    client.close()
    ti.xcom_push(key='btc_usd', value={'status': f'{btc_usd} pushed'})


with DAG(
        'yandex_test',
        default_args={
            'depends_on_past': False,
            'email': ['armanslaptop@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
        },
        schedule_interval=timedelta(hours=3),
        start_date=datetime.utcnow(),
        tags=['practikum'],
) as dag:
    extract = PythonOperator(
        task_id='api_request',
        python_callable=make_requests
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=handle_data
    )

    load_mongo = PythonOperator(
        task_id='load_mongo',
        python_callable=load_data
    )

    extract >> transform >> load_mongo
