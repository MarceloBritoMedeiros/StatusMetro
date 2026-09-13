import logging

from flask import Flask, jsonify

from config import API_CALL, GCP_TOKEN, TABLE, TOKEN, USERS_TABLE
from firebase_service import initialize_firebase
from metro_service import check_status_updates


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

collection_ref, users_table = initialize_firebase(GCP_TOKEN, TABLE, USERS_TABLE)

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    """Retorna uma mensagem indicando que a API está disponível."""
    return jsonify({"message": "Servidor MetroAPI funcionando!"})


@app.route("/health", methods=["GET"])
def health_check():
    """Retorna o estado de saúde da aplicação."""
    return jsonify({"status": "ok"})


@app.route("/update", methods=["GET"])
def result():
    """Executa a atualização dos status e retorna o resultado do processamento."""
    result_data = check_status_updates(API_CALL, TOKEN, collection_ref, users_table)
    logging.info("Resultado da atualização: %s", result_data)
    return jsonify({"message": result_data})


# if __name__ == "__main__":
#     app.run(debug=True)
