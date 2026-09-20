import requests


def send_telegram_message(token, chat_id, message):
    """Envia uma mensagem de texto para um chat do Telegram.

    Args:
        token: Token do bot do Telegram.
        chat_id: Identificador do chat destinatário.
        message: Texto que será enviado.

    Returns:
        Mensagem indicando que o envio foi concluído.

    Raises:
        requests.exceptions.RequestException: Se o Telegram rejeitar a requisição.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": message,
    }

    response = requests.post(url, data=params, timeout=10)
    response.raise_for_status()
    return "Mensagem enviada com sucesso!"
