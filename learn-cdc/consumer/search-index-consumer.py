import json
import logging
import sys 

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaConsumer
from cdc_common import BOOTSTRAP_SERVERS, TOPICS, PK_FIELD, parse_event

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("search_index_consumer")

# Which fields actually get indexed for search, per table — a real search
# index rarely mirrors every DB column; it usually picks (and sometimes
# renames/combines) just the fields worth searching on.
SEARCHABLE_FIELDS = {
    "customers": ["first_name", "last_name", "email"],
    "orders": ["product_id", "status", "total_amount"],
}

def build_search_document(table: str, row: dict) -> dict:
    """Select + transform only the fields worth indexing for search.
    A real search sync would also denormalize related data here — e.g.
    pulling in the customer's name onto an order document — but that's
    a good next step once this basic version is working."""
    fields = SEARCHABLE_FIELDS.get(table, [])
    doc = {f: row.get(f) for f in fields if f in row}

    # simple derived field, showing "transformation" isn't just 1:1 copying
    if table == "customers":
        doc["full_name"] = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()

    return doc

def handle_event(event: dict):
    table = event["table"]
    op = event["op"]
    pk_field = PK_FIELD.get(table)
    if not pk_field:
        return

    if op == "d":
        doc_id = event["before"].get(pk_field)
        log.info(f"[SEARCH-DELETE] index={table} doc_id={doc_id}")
        return

    row = event["after"]
    doc_id = row.get(pk_field)
    if doc_id is None:
        return

    doc = build_search_document(table, row)
    log.info(f"[SEARCH-UPSERT] index={table} doc_id={doc_id}\n  {doc}")

def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        # Different group_id from cdc_consumer.py — this is what makes it
        # an independent reader of the same topics, with its own offset
        # tracking, rather than competing with the other consumer.
        group_id="cdc_search_index_consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    log.info(f"[search-index-consumer] Listening on: {TOPICS}\n")

    for message in consumer:
        if message.value is None:
            continue

        event = parse_event(message.value)
        if event is None:
            continue

        handle_event(event)


if __name__ == "__main__":
    main()