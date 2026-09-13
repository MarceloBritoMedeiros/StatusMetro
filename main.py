import json
import os

import firebase_admin
import numpy as np
import pandas as pd
import requests
from firebase_admin import credentials, firestore
from flask import Flask, jsonify


GCP_TOKEN = os.getenv("GCP_TOKEN")
TOKEN = os.getenv("MEU_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TABLE = os.getenv("TABLE", "status_metro_temp")
USERS_TABLE = os.getenv("USERS_TABLE", "users")
API_CALL = os.getenv(
    "API_CALL",
    "http://apps.cptm.sp.gov.br:8080/AppMobileService/api/"
    "LinhasMetropolitanasAppV3?versao=4",
)

if not GCP_TOKEN:
    raise RuntimeError("A variável de ambiente GCP_TOKEN não foi definida.")

if not TOKEN:
    raise RuntimeError("A variável de ambiente MEU_TOKEN não foi definida.")

if not CHAT_ID:
    raise RuntimeError("A variável de ambiente CHAT_ID não foi definida.")

if not TABLE:
    raise RuntimeError("A variável de ambiente TABLE não foi definida.")

if not firebase_admin._apps:
    cred = credentials.Certificate(GCP_TOKEN)
    firebase_admin.initialize_app(cred)

db = firestore.client()
colecao_ref = db.collection(TABLE)
users_table = db.collection(USERS_TABLE)


def read_firebase_as_pd():
    docs = colecao_ref.stream()
    lista_dados = []

    for doc in docs:
        dados_doc = doc.to_dict()
        dados_doc["id"] = doc.id
        lista_dados.append(dados_doc)

    return pd.DataFrame(lista_dados)


def gera_status(metro_call):
    metro_call_dict = json.loads(metro_call)
    df = pd.DataFrame(metro_call_dict)
    df_salvo = read_firebase_as_pd()

    if df_salvo.empty:
        return []

    cruza1 = pd.merge(
        df,
        df_salvo,
        how="inner",
        on="LinhaId",
        suffixes=("", "_b"),
    )

    cruza1["mudou"] = np.where(
        cruza1["Status"] != cruza1["Status_b"],
        1,
        0,
    )

    cruza = cruza1[
        (cruza1["mudou"] == 1)
        & (
            ~cruza1["Status"].isin(
                [
                    "Operação Encerrada",
                    "Operações Encerradas",
                    "Informação indisponível",
                ]
            )
        )
    ]

    if cruza.empty:
        return []

    status_antigo = cruza["Status_b"].tolist()
    status_novo = cruza["Status"].tolist()
    linhaid = cruza["LinhaId"].tolist()
    nome = cruza["Nome"].tolist()
    descricao = cruza["Descricao"].tolist()

    cores = {
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
    }

    lista = []

    for sa, sn, l, n, d in zip(
        status_antigo,
        status_novo,
        linhaid,
        nome,
        descricao,
    ):
        cor = cores.get(str(n).upper(), "🚇")
        descricao_texto = f"\n\nDescrição: {d}" if d else ""

        lista.append(
            f"⚠️ Alteração no status da Linha {l} - {n} {cor}\n"
            f"\n🔄 Status atual: {sn} "
            f"{'✅' if sn == 'Operação Normal' else '⚠️'}"
            f"\n📍 Status anterior: {sa}"
            f"{descricao_texto}"
        )

    # Atualiza somente os registros que sofreram alteração.
    for item in cruza.to_dict(orient="records"):
        doc_ref = colecao_ref.document(str(item["LinhaId"]))
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

    return lista


def faz_request():
    response = requests.get(API_CALL, timeout=30)
    response.raise_for_status()

    mensagens = gera_status(response.text)
    retorno = []

    # Mantém o CHAT_ID principal.
    # Para buscar usuários do Firestore, consulte users_table
    # e adicione os IDs à lista abaixo.
    ids = [CHAT_ID]

    if mensagens:
        for chat_id in ids:
            for mensagem in mensagens:
                url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
                params = {
                    "chat_id": chat_id,
                    "text": mensagem,
                }

                try:
                    response = requests.post(
                        url,
                        data=params,
                        timeout=30,
                    )
                    response.raise_for_status()
                    retorno.append("Mensagem enviada com sucesso!")
                except requests.exceptions.RequestException as error:
                    retorno.append(f"Ocorreu um erro: {error}")

        return retorno

    return "Sem mudanças"


app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Servidor MetroAPI funcionando!"})


@app.route("/update", methods=["GET"])
def result():
    resultado = faz_request()
    print(resultado)
    return jsonify({"message": resultado})


if __name__ == "__main__":
    app.run(debug=True)
