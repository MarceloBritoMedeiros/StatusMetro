import json
import logging

import numpy as np
import pandas as pd

from firebase_service import read_firebase_as_dataframe, update_status_documents

IGNORED_STATUSES = {
    "Operação Encerrada",
    "Operações Encerradas",
    "Informação indisponível",
}

LINE_COLORS = {
    "AZUL": "🔵",
    "VERDE": "🟢",
    "VERMELHA": "🔴",
    "AMARELA": "🟡",
    "LILÁS": "🟣",
    "RUBI": "🔴",
    "DIAMANTE": "⚪",
    "ESMERALDA": "🟢",
    "TURQUESA": "🔵",
    "CORAL": "🟠",
    "SAFIRA": "🔵",
    "JADE": "🟢",
    "PRATA": "⚪",
    "OURO": "🟡",
    "LARANJA": "🟠",
}

REQUIRED_STATUS_COLUMNS = {"LinhaId", "Nome", "Status", "Descricao", "DataGeracao", "Tipo"}


def normalize_status_row(row):
    """Normaliza os campos de uma linha para comparação consistente.

    Args:
        row: Série do pandas contendo os dados de uma linha.

    Returns:
        Série com os campos textuais limpos e convertidos para string.
    """
    normalized = row.copy()
    normalized["LinhaId"] = str(normalized.get("LinhaId", "")).strip()
    normalized["Nome"] = str(normalized.get("Nome", "")).strip()
    normalized["Status"] = str(normalized.get("Status", "")).strip()
    normalized["Descricao"] = str(normalized.get("Descricao", "") or "").strip()
    normalized["Tipo"] = str(normalized.get("Tipo", "")).strip()
    return normalized


def build_status_message(line_id, line_name, previous_status, current_status, description):
    """Monta a mensagem de alerta para uma alteração de status.

    Args:
        line_id: Identificador da linha.
        line_name: Nome da linha.
        previous_status: Status anteriormente registrado.
        current_status: Status atual.
        description: Descrição adicional da ocorrência.

    Returns:
        Texto formatado para envio ao Telegram.
    """
    color = LINE_COLORS.get(str(line_name).upper(), "")
    status_icon = "✅" if current_status == "Operação Normal" else "⚠️"
    description_text = f"\n\n Descrição: {description}" if description else ""

    return (
        f"⚠️ Alteração no status da Linha {line_id} - {line_name} {color}\n"
        f"\n 🔄Status atual: {current_status}{status_icon}"
        f"\n📍Status anterior: {previous_status}{description_text}"
    )


def generate_status(metro_call, collection_ref):
    """Compara o status atual com o histórico e gera os alertas necessários.

    Args:
        metro_call: Resposta JSON da API da CPTM.
        collection_ref: Referência da coleção de status no Firestore.

    Returns:
        Lista de mensagens para as alterações relevantes encontradas.
    """
    try:
        metro_call_dict = json.loads(metro_call)
    except json.JSONDecodeError as exc:
        logging.error("Resposta da API da CPTM inválida: %s", exc)
        return []

    if not isinstance(metro_call_dict, list):
        logging.warning("Resposta da API da CPTM não é uma lista: %s", type(metro_call_dict).__name__)
        return []

    current_status = pd.DataFrame(metro_call_dict)
    if current_status.empty:
        return []

    missing_columns = REQUIRED_STATUS_COLUMNS - set(current_status.columns)
    if missing_columns:
        logging.warning("Campos ausentes na API da CPTM: %s", sorted(missing_columns))
        return []

    current_status = current_status.apply(normalize_status_row, axis=1)
    previous_status = read_firebase_as_dataframe(collection_ref)

    if previous_status.empty:
        logging.info("Sem dados prévios no Firestore para comparar.")
        return []

    previous_status = previous_status.apply(normalize_status_row, axis=1)

    merged_status = pd.merge(
        current_status,
        previous_status,
        how="inner",
        on="LinhaId",
        suffixes=("", "_b"),
    )

    if merged_status.empty:
        return []

    merged_status["changed"] = np.where(
        merged_status["Status"] != merged_status["Status_b"],
        1,
        0,
    )

    status_result = merged_status[
        (merged_status["changed"] == 1)
        & (~merged_status["Status"].isin(list(IGNORED_STATUSES)))
    ]

    if status_result.empty:
        return []

    update_status_documents(status_result, collection_ref)

    return [
        build_status_message(
            row["LinhaId"],
            row["Nome"],
            row["Status_b"],
            row["Status"],
            row["Descricao"],
        )
        for _, row in status_result.iterrows()
    ]
