import json
import os


GCP_TOKEN = json.loads(os.getenv("GCP_TOKEN"))
TOKEN = os.getenv("MEU_TOKEN")
TABLE = os.getenv("TABLE")
USERS_TABLE = os.getenv("USERS_TABLE")
API_CALL = os.getenv("API_CALL")
