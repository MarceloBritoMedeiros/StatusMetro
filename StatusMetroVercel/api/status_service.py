import json
import logging

import numpy as np
import pandas as pd

from firebase_service import read_firebase_as_dataframe, update_status_documents

# Estes status são ignorados porque não representam uma alteração útil para o usuário,
# como linhas encerradas ou informações indisponíveis para a operação normal.
IGNORED_STATUSES = {
    "Operação Encerrada",
    "Operações Encerradas",
    "Informação indisponível",
}

# Mapeamento do nome da linha para um símbolo visual que ajuda a identificar rapidamente
# a cor da linha no alerta enviado ao Telegram.
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

# Lista mínima de colunas que devem existir na resposta da CPTM.
# Isso evita erros de chave quando algum campo vier ausente ou com nome diferente.
REQUIRED_STATUS_COLUMNS = {"LinhaId", "Nome", "Status", "Descricao", "DataGeracao", "Tipo"}


# Normaliza cada linha recebida da API para remover espaços vazios e garantir que os textos
# fiquem consistentes antes de comparar com o conteúdo salvo no Firestore.
def normalize_status_row(row):
    normalized = row.copy()
    normalized["LinhaId"] = str(normalized.get("LinhaId", "")).strip()
    normalized["Nome"] = str(normalized.get("Nome", "")).strip()
    normalized["Status"] = str(normalized.get("Status", "")).strip()
    normalized["Descricao"] = str(normalized.get("Descricao", "") or "").strip()
    normalized["Tipo"] = str(normalized.get("Tipo", "")).strip()
    return normalized


# Constrói a mensagem enviada para o Telegram.
# A função usa o nome da linha, a cor correspondente e o status anterior/atual para criar
# um alerta legível para o usuário final.
def build_status_message(line_id, line_name, previous_status, current_status, description):
    color = LINE_COLORS.get(str(line_name).upper(), "")
    status_icon = "✅" if current_status == "Operação Normal" else "⚠️"
    description_text = f"\n\n Descrição: {description}" if description else ""

    return (
        f"⚠️ Alteração no status da Linha {line_id} - {line_name} {color}\n"
        f"\n 🔄Status atual: {current_status}{status_icon}"
        f"\n📍Status anterior: {previous_status}{description_text}"
    )


# Essa é a função principal de comparação.
# Ela recebe o payload da CPTM, lê os dados salvos no Firestore, encontra diferenças e
# atualiza o banco quando uma linha mudou de status.
def generate_status(metro_call, collection_ref):
    # Tenta transformar o retorno em JSON. Se falhar, o serviço deve continuar em segurança
    # em vez de quebrar a execução por uma resposta malformada.
    try:
        metro_call_dict = json.loads(metro_call)
    except json.JSONDecodeError as exc:
        logging.error("Resposta da API da CPTM inválida: %s", exc)
        return []

    # A API precisa responder como lista para que o processamento faça sentido.
    if not isinstance(metro_call_dict, list):
        logging.warning("Resposta da API da CPTM não é uma lista: %s", type(metro_call_dict).__name__)
        return []

    # Cria um DataFrame a partir da resposta e valida se ele não está vazio.
    current_status = pd.DataFrame(metro_call_dict)
    if current_status.empty:
        return []

    # Verifica se todas as colunas essenciais existem antes de tentar acessar os dados.
    missing_columns = REQUIRED_STATUS_COLUMNS - set(current_status.columns)
    if missing_columns:
        logging.warning("Campos ausentes na API da CPTM: %s", sorted(missing_columns))
        return []

    # Normaliza os valores para remover espaços e preparar o DataFrame para merge.
    current_status = current_status.apply(normalize_status_row, axis=1)
    previous_status = read_firebase_as_dataframe(collection_ref)

    # Se não há histórico salvo, não há como comparar o estado anterior.
    if previous_status.empty:
        logging.info("Sem dados prévios no Firestore para comparar.")
        return []

    previous_status = previous_status.apply(normalize_status_row, axis=1)

    # Realiza o merge entre o status atual e o salvo no banco usando a chave LinhaId.
    merged_status = pd.merge(
        current_status,
        previous_status,
        how="inner",
        on="LinhaId",
        suffixes=("", "_b"),
    )

    # Se o merge não encontrar linhas em comum, não há alteração para reportar.
    if merged_status.empty:
        return []

    # Cria uma coluna booleana para indicar se o status mudou em relação ao anterior.
    merged_status["changed"] = np.where(
        merged_status["Status"] != merged_status["Status_b"],
        1,
        0,
    )

    # Filtra apenas alterações relevantes e ignora os status que não geram alerta.
    status_result = merged_status[
        (merged_status["changed"] == 1)
        & (~merged_status["Status"].isin(list(IGNORED_STATUSES)))
    ]

    # Se não houve mudança relevante, retorna lista vazia.
    if status_result.empty:
        return []

    # Atualiza os dados no Firestore para sincronizar o último status conhecido.
    update_status_documents(status_result, collection_ref)

    # Gera uma mensagem por linha alterada para enviar ao Telegram.
    messages = []
    for _, row in status_result.iterrows():
        messages.append(
            build_status_message(
                row["LinhaId"],
                row["Nome"],
                row["Status_b"],
                row["Status"],
                row["Descricao"],
            )
        )

    return messages
