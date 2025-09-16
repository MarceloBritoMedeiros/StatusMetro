import asyncio
import aiohttp
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import json 
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os

TOKEN = os.getenv("MEU_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")  # Onde o bot enviará as mensagens

import requests

x = requests.get("http://apps.cptm.sp.gov.br:8080/AppMobileService/api/LinhasMetropolitanasAppV3?versao=4").text
def gera_status(x):
    y = json.loads(x)
    df = pd.DataFrame(y)
    df_salvo = pd.read_csv("status_metro.csv")
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

    print("Salvando status_metro.csv")
    df.to_csv("status_metro.csv")
    # print(cruza[cruza["mudou"]==0].empty)
    return lista

# while True:
mensagens = gera_status(x)
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
            print("Mensagem enviada com sucesso!")
        except requests.exceptions.RequestException as e:
            print(f"Ocorreu um erro: {e}")
else:
    print("Sem mudanças")
    # print(datetime.now().strftime("%H:%M:%S"))
    # time.sleep(3600)


# Exemplo de função para buscar dados da sua API
# async def fetch_api_data():
#     url = "URL_DA_SUA_API"
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as response:
#             return await response.json()  # Ajuste conforme a API

# Função que envia mensagem
# Função que envia mensagem a cada hora
# async def send_message(app):
#     while True:
#         try:
#             mensagem = "Mensagem teste"
#             await app.bot.send_message(chat_id=CHAT_ID, text=mensagem)
#             print("Mensagem enviada")
#         except Exception as e:
#             print(f"Erro ao enviar mensagem: {e}")
#         await asyncio.sleep(3600)  # 1 hora

# # Handler do /start
# async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     await update.message.reply_text("Bot iniciado! Você receberá mensagens a cada hora.")

# # Função principal
# async def main():
#     app = ApplicationBuilder().token(TOKEN).build()
#     app.add_handler(CommandHandler("start", start))

#     # Cria tarefa de envio periódico
#     asyncio.create_task(send_message(app))

#     # Rodar o bot até ser interrompido
#     await app.run_polling()

# # Executa
# asyncio.run(main())