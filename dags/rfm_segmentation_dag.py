import requests
from airflow import DAG

TELEGRAM_TOKEN = "8663739718:AAGVM2NyXhkO0s6l9NWqHs6xmeSwuNxVTfI"
TELEGRAM_CHAT_ID = "1114691272"

def send_telegram_alert(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url
    
    message = (
        f"<b>Airflow Task Failed!</b>\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Logs:</b> {log_url}"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'on_failure_callback': send_telegram_alert
}
