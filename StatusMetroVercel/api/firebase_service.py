import pandas as pd

import firebase_admin
from firebase_admin import credentials, firestore


def initialize_firebase(gcp_token, table, users_table):
    """Inicializa o Firebase e retorna as coleções usadas pela aplicação.

    Args:
        gcp_token: Credencial da conta de serviço do Google Cloud.
        table: Nome da coleção que armazena os status das linhas.
        users_table: Nome da coleção que armazena os usuários.

    Returns:
        Uma tupla contendo as referências das coleções de status e usuários.
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate(gcp_token)
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    return db.collection(table), db.collection(users_table)


def read_firebase_as_dataframe(collection_ref):
    """Lê uma coleção do Firestore e converte seus documentos em DataFrame.

    Args:
        collection_ref: Referência da coleção que será lida.

    Returns:
        DataFrame contendo os documentos encontrados na coleção.
    """
    docs = collection_ref.stream()
    records = []

    for doc in docs:
        record = doc.to_dict()
        record["id"] = doc.id
        records.append(record)

    return pd.DataFrame(records)


def update_status_documents(status_result, collection_ref):
    """Atualiza no Firestore os documentos correspondentes aos status alterados.

    Args:
        status_result: DataFrame com as linhas que tiveram alteração.
        collection_ref: Referência da coleção de status.
    """
    for item in status_result.to_dict(orient="records"):
        doc_ref = collection_ref.document(str(item["LinhaId"]))
        doc_ref.update(
            {
                "Nome": item["Nome"],
                "DataGeracao": item["DataGeracao"],
                "Status": item["Status"],
                "Tipo": item["Tipo"],
                "Descricao": item["Descricao"],
                "LinhaId": item["LinhaId"],
            }
        )
