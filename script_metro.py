import aiohttp
import asyncio
import requests
import pandas as pd
import json
import numpy as np

def gera_status(x):
    y = x#json.loads(x)
    df = pd.DataFrame(y)
    df_salvo = pd.read_csv("status_metro.csv")
    cruza = pd.merge(df, df_salvo, "inner", "LinhaId", suffixes=("_a", "_b"))

    cruza["mudou"] = np.where(cruza["Status_a"] != cruza["Status_b"], 1, 0)

    status_antigo = cruza["Status_a"].tolist()
    status_novo = cruza["Status_b"].tolist()
    linhaid = cruza["LinhaId"].tolist()
    nome = cruza["Nome_a"].tolist()
    descricao = cruza["Descricao_b"].tolist()
    lista = []

    for sa, sn, l, n, d in zip(status_antigo, status_novo, linhaid, nome, descricao):
        lista.append(f"Houve mudança do status da linha {l} {n}: \n Status antigo: {sa}\n Status atual: {sn}\n Descrição: {d}")

    print("Salvando status_metro.csv")
    df.to_csv("status_metro.csv")
    # print(cruza[cruza["mudou"]==0].empty)
    return lista

url = "http://apps.cptm.sp.gov.br:8080/AppMobileService/api/LinhasMetropolitanasAppV3?versao=4"

async def fetch_api_data():
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()  # <--- Aqui precisa do await
            print(type(data))
            y = gera_status(data)                  # Agora imprime os dados corretos
            print(y)
            return y

asyncio.run(fetch_api_data())




