import json 
import pandas as pd
import numpy as np
import os
import firebase_admin
from firebase_admin import credentials, firestore
import requests


GCP_TOKEN = os.getenv("GCP_TOKEN")
TOKEN = os.getenv("MEU_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")  # Onde o bot enviará as mensagens
TABLE = os.getenv("TABLE")
API_CALL = os.getenv("API_CALL")

# Inicializa o Firebase
cred = credentials.Certificate(GCP_TOKEN)
firebase_admin.initialize_app(cred)

db = firestore.client()
colecao_ref = db.collection(TABLE)

metro_call = requests.get(API_CALL).text

def read_firebase_as_pd():  
    docs = colecao_ref.stream()

    lista_dados = []  

    for doc in docs:
        dados_doc = doc.to_dict()
        dados_doc["id"] = doc.id  # opcional: adiciona o ID do documento
        lista_dados.append(dados_doc)

    # Converter para DataFrame
    df = pd.DataFrame(lista_dados)
    return df

df = read_firebase_as_pd()


def gera_status(metro_call):
    metro_call_dict = json.loads(metro_call)
    df = pd.DataFrame(metro_call_dict)
    df_salvo = read_firebase_as_pd()
    cruza1 = pd.merge(df, df_salvo, "inner", "LinhaId", suffixes=("_a", "_b"))

    cruza1["mudou"] = np.where(cruza1["Status_a"] != cruza1["Status_b"], 1, 0)
    cruza = cruza1[cruza1["mudou"]==1]

    status_antigo = cruza["Status_b"].tolist()
    status_novo = cruza["Status_a"].tolist()
    linhaid = cruza["LinhaId"].tolist()
    nome = cruza["Nome_a"].tolist()
    descricao = cruza["Descricao_a"].tolist()
    lista = []

    for sa, sn, l, n, d in zip(status_antigo, status_novo, linhaid, nome, descricao):
        lista.append(f"Houve mudança do status da linha {l} {n}: \n Status antigo: {sa}\n Status atual: {sn}\n Descrição: {d}")

    #Atualiza a tabela temporária
    for i in metro_call_dict:
        doc_ref = colecao_ref.document(str(i["LinhaId"]))
        doc_ref.update({
            'Nome':i["Nome"],
            'DataGeracao':i["DataGeracao"],
            'Status':i["Status"],
            'Tipo':i["Tipo"],
            'Descricao':i["Descricao"],
            'LinhaId':i["LinhaId"],
        })
    return lista

def request_body(message):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": f'{"message": {message}}'
    }

# while True:
def handler(request):
    mensagens = gera_status(metro_call)
    retorno = []
    if mensagens:
        for i in mensagens:
            # Substitua pelas suas informações
            MENSAGEM = i#'Olá, mundo! Esta é uma mensagem automática.'

            # URL da API para enviar mensagens
            url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'

            # Parâmetros da requisição
            params = {
                'chat_id': CHAT_ID,
                'text': MENSAGEM
            }

            # Faz a requisição POST para a API
            try:
                response = requests.post(url, data=params)
                response.raise_for_status()  # Lança um erro para requisições HTTP ruins
                retorno.append("Mensagem enviada com sucesso!")
            except requests.exceptions.RequestException as e:
                retorno.append(f"Ocorreu um erro: {e}")
        return request_body(retorno)
        
    else:
        return request_body("Sem mudanças")

