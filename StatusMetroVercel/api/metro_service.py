import logging

import requests

from api.status_service import generate_status
from telegram_service import send_telegram_message


def fetch_metro_status(api_call):
    """Consulta a API da CPTM e retorna sua resposta em texto.

    Args:
        api_call: URL da API que será consultada.

    Returns:
        Conteúdo textual retornado pela API.

    Raises:
        requests.exceptions.RequestException: Se a consulta falhar.
    """
    try:
        response = requests.get(api_call, timeout=15)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        logging.exception("Erro ao consultar a API da CPTM")
        raise


def check_status_updates(api_call, token, collection_ref, users_table):
    """Processa alterações de status e envia alertas aos usuários cadastrados.

    Args:
        api_call: URL da API da CPTM.
        token: Token do bot do Telegram.
        collection_ref: Referência da coleção de status no Firestore.
        users_table: Referência da coleção de usuários no Firestore.

    Returns:
        Mensagem ou lista com os resultados do processamento.
    """
    try:
        metro_call = fetch_metro_status(api_call)
    except requests.exceptions.RequestException as exc:
        logging.exception("Falha ao buscar status da CPTM")
        return f"Erro ao consultar a API da CPTM: {exc}"

    messages = generate_status(metro_call, collection_ref)

    if not messages:
        return "Sem mudanças"

    docs = users_table.stream()
    chat_ids = [doc.id for doc in docs]

    if not chat_ids:
        return "Nenhum usuário cadastrado"

    response = []

    for chat_id in chat_ids:
        for message in messages:
            try:
                response.append(send_telegram_message(token, chat_id, message))
            except requests.exceptions.RequestException as exc:
                logging.exception("Erro ao enviar mensagem para o chat %s", chat_id)
                response.append(f"Ocorreu um erro: {exc}")

    return response
