import os 
import json 
import requests
from dotenv import load_dotenv

#Python ที่ไปสั่ง Kafka Connect ให้สร้าง Debezium PostgreSQL Connector
"""
โค้ดนี้ ไม่ได้อ่านข้อมูล
ไม่ได้ process data
แต่ทำหน้าที่เป็น Control Plane → ไป “สร้าง connector” ผ่าน REST API
"""
# Load environment variables from .env file
load_dotenv()

# -----------------------------------------------------------------
# create connector config with json format in the memory
# -----------------------------------------------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "topic.prefix": "banking_server", 
        "table.include.list" : "public.customers,public.accounts,public.transactions", 
        "plugin.name" : "pgoutput",
        "slot.name" : "banking_slot",
        "publication.autocreate.mode" : "filtered",
        "tombstones.on.delete": "false",
        #not convert decimal to string, but use double
        "decimal.handling.mode": "double",

    }
}

# -----------------------------------------------------------------
# send a request to Kafka Connect REST API to create the connector
# -----------------------------------------------------------------
connect_url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}
response = requests.post(
    connect_url,
    headers=headers,
    data=json.dumps(connector_config)
)

# -----------------------------------------------------------------
# print the response from Kafka Connect
# -----------------------------------------------------------------
if response.status_code == 201:
    print("Connector created successfully.")
elif response.status_code == 409:
    print("Connector already exists.")
else:
    print(f"Failed to create connector. Status code: {response.status_code}")
    print(f"Response: {response.text}")   