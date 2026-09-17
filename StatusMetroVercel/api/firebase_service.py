import pandas as pd


# Lê todos os documentos da coleção do Firestore e converte para DataFrame.
def read_firebase_as_dataframe(collection_ref):
    docs = collection_ref.stream()
    records = []

    for doc in docs:
        record = doc.to_dict()
        record["id"] = doc.id
        records.append(record)

    return pd.DataFrame(records)


# Atualiza os documentos que tiveram mudança no status para manter o banco sincronizado.
def update_status_documents(status_result, collection_ref):
    for item in status_result.to_dict(orient="records"):
        doc_ref = collection_ref.document(str(item["LinhaId"]))
        doc_ref.set(
            {
                "Nome": item["Nome"],
                "DataGeracao": item["DataGeracao"],
                "Status": item["Status"],
                "Tipo": item["Tipo"],
                "Descricao": item["Descricao"],
                "LinhaId": item["LinhaId"],
            },
            merge=True,
        )
