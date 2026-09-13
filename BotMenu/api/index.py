from flask import Flask, request, jsonify
import requests
import os
import firebase_admin
from firebase_admin import credentials, firestore
import json

app = Flask(__name__)

# from dotenv import load_dotenv
# load_dotenv()

TOKEN = os.getenv("MEU_TOKEN")

BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

GCP_TOKEN = json.loads(os.getenv("GCP_TOKEN"))


USERS_TABLE = os.getenv("USERS_TABLE")


# Inicializa o Firebase
cred = credentials.Certificate(GCP_TOKEN)
firebase_admin.initialize_app(cred)

db = firestore.client()
colecao_ref = db.collection(USERS_TABLE)

# Mensagem de boas-vindas + Reply Keyboard
def send_welcome(chat_id):
    text = "Olá! Bem-vindo ao StatusMetroBot🚇. Aqui você receberá notificações em tempo real sempre que houver qualquer desvio nas linhas de metrô, e será informado assim que as atividades voltarem ao normal.\nClique no botão /menu para ver as opções disponíveis."
    reply_keyboard = {
        "keyboard": [["Menu"], ["Status Atual"]],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": reply_keyboard
    }
    requests.post(f"{BASE_URL}/sendMessage", json=payload)

# Mensagem com Inline Keyboard (Cadastrar / Descadastrar)
def send_menu(chat_id):
    doc = colecao_ref.document(str(chat_id)).get()
    print(doc.exists)
    if doc.exists:
        text = "Você já está cadastrado no sistema. Gostaria de se descadastrar?:"
        inline_keyboard = {
            "inline_keyboard": [
                [{"text": "Descadastrar", "callback_data": "descadastrar"}]
            ]
        }
    else:
        text = "Você ainda não está cadastrado no sistema. Gostaria de se cadastrar?"
        inline_keyboard = {
            "inline_keyboard": [
                [{"text": "Cadastrar", "callback_data": "cadastrar"}]
            ]
        }
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": inline_keyboard
    }
    requests.post(f"{BASE_URL}/sendMessage", json=payload)

# Mensagem simples
def send_message(chat_id, text):
    payload = {"chat_id": chat_id, "text": text}
    requests.post(f"{BASE_URL}/sendMessage", json=payload)

def send_current_status(chat_id):
    x = requests.get("http://apps.cptm.sp.gov.br:8080/AppMobileService/api/LinhasMetropolitanasAppV3?versao=4").text
    cores = {
        "AZUL": "🔵",
        "VERDE": "🟢",
        "VERMELHA": "🔴",
        "AMARELA": "🟡",
        "LILÁS": "🟣",
        "RUBI": "🔴",       # rubi = vermelho
        "DIAMANTE": "⚪",   # diamante = branco/transparente
        "ESMERALDA": "🟢",  # esmeralda = verde
        "TURQUESA": "🔵",   # turquesa = azul claro
        "CORAL": "🟠",      # coral = laranja/rosa
        "SAFIRA": "🔵",     # safira = azul
        "JADE": "🟢",       # jade = verde
        "PRATA": "⚪"       # prata = bolinha branca (não existe cinza metálico)
    }

    text = "Linhas:\n"

    for i in json.loads(x):
        text += (        
            f"{cores[i['Nome']]}L{i['LinhaId']} - {i['Nome'].capitalize()}: {i['Status']} "
            f"{'✅' if i['Status']=='Operação Normal' else '⚠️'}"
            f"{'\n🚨Problema: ' + i['Descricao'] if i['Descricao'] != '' else ''}\n"
        )
    
    reply_keyboard = {
        "keyboard": [["Menu"], ["Status Atual"]],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": reply_keyboard
    }
    requests.post(f"{BASE_URL}/sendMessage", json=payload)
    
@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Menu do MetroAPI funcionando!"})

@app.route(f"/webhook/{TOKEN}", methods=["POST", "GET"])
def webhook():
    data = request.get_json()

    # Mensagem normal
    if "message" in data:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"].get("text", "")

        if text.lower() in ("/start", "/start welcome"):
            send_welcome(chat_id)
        elif text.lower() in ("/menu", "menu"):
            send_menu(chat_id)
        elif text.lower() == "status atual":
            send_current_status(chat_id)
        else:
            send_message(chat_id, f"Você disse: {text}")

    # Clique em botão inline
    elif "callback_query" in data:
        chat_id = data["callback_query"]["message"]["chat"]["id"]
        query_data = data["callback_query"]["data"]

        if query_data == "cadastrar":
            # Referência ao documento
            doc_ref = colecao_ref.document(str(chat_id))

            # Criar documento vazio
            doc_ref.set({})
            
            #Mandar mensagem
            send_message(chat_id, "Você foi cadastrado nas notificações ✅")
        elif query_data == "descadastrar":
            # Referência ao documento
            doc_ref = colecao_ref.document(str(chat_id))

            # Deletar o documento
            doc_ref.delete()
            
            #Mandar mensagem
            send_message(chat_id, "Você foi descadastrado das notificações ❌")

    return {"ok": True}

# if __name__ == "__main__":
#     app.run(port=5000, debug=True)