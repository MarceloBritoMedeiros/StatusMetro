import requests


# Envia uma mensagem de texto para um chat específico do Telegram.
def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": message,
    }

    response = requests.post(url, data=params, timeout=10)
    response.raise_for_status()
    return "Mensagem enviada com sucesso!"
