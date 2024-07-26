import os
import json


def get_service_account_info():
    return json.loads(os.environ["GOOGLE_SQLMESH_CREDENTIALS"])
