import os 
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPICS = ["cdc_demo.public.customers", "cdc_demo.public.orders"]

# Which field to use as the short identifier per table, for one-line summaries
PK_FIELD = {
    "customers": "customer_id",
    "orders": "order_id",
}

DB_CONFIG = { 
    "host" : os.getenv("POSTGRES_HOST"),
    "port" : os.getenv("POSTGRES_PORT"), 
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

def parse_event(raw_value: dict):
    """Extract the useful bits out of a Debezium change event envelope."""
    payload = raw_value.get("payload") if "payload" in raw_value else raw_value
    if payload is None:
        return None

    op = payload.get("op")
    before = payload.get("before") or {}
    after = payload.get("after") or {}
    source = payload.get("source", {})

    return {
        "op": op,
        "table": source.get("table"),
        "before": before,
        "after": after,
    }