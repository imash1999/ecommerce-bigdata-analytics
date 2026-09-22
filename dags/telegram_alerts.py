import urllib.request
import json
import ssl

TELEGRAM_TOKEN = '8597372280:AAE7P0-0u8aNzdUqzYQcjhajwIq4H_dpaHk'
TELEGRAM_CHAT_ID = '1114691272'

def send_telegram_failure_alert(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date').strftime("%Y-%m-%d %H:%M:%S")
    exception = context.get('exception')

    message = (
        f"🚨 <b>Airflow Task Failed!</b>\n\n"
        f"<b>DAG:</b> <code>{dag_id}</code>\n"
        f"<b>Task:</b> <code>{task_id}</code>\n"
        f"<b>Time:</b> {execution_date}\n"
        f"<b>Error:</b> <code>{exception}</code>"
    )

    # Используем IP-адрес Telegram API напрямую, чтобы обойти DNS-блокировку WSL
    url = f"https://149.154.167.220/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }).encode('utf-8')

    # Отключаем строгую проверку сертификата для IP-адреса
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(
        url, 
        data=payload, 
        headers={
            'Content-Type': 'application/json',
            'Host': 'api.telegram.org'
        }
    )
    
    try:
        urllib.request.urlopen(req, context=ctx, timeout=10)
        print("Алерт успешно отправлен!")
    except Exception as e:
        print(f"Ошибка отправки Telegram алерта: {e}")
