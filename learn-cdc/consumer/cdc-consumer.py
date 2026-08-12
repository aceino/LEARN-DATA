import json
import logging
import threading
import sys

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaConsumer
from cdc_common import BOOTSTRAP_SERVERS, TOPICS, PK_FIELD, parse_event

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("cdc_consumer")

# Debezium op codes -> human-readable labels + ANSI colors for quick scanning
OP_STYLE = {
    "c": ("CREATE", "\033[92m"),  # green
    "r": ("SNAPSHOT", "\033[94m"),  # blue
    "u": ("UPDATE", "\033[93m"),  # yellow
    "d": ("DELETE", "\033[91m"),  # red
}

RESET = "\033[0m"

CACHE_STATS = { 
    "hits" : 0,
    "misses": 0, 
    "updates" : 0, 
    "deletes" : 0, 
    "inserts" : 0
}

# In-memory materialized view: {table_name: {primary_key_value: row_dict}}
# This is a live snapshot of "current state," rebuilt entirely from the
# CDC event stream — no direct queries to Postgres involved.
CACHE = {}

def update_cache(event: dict) : 
    table = event["table"]

    if table not in CACHE : 
        CACHE[table] = {}

    pk_field =PK_FIELD.get(table)
    if not pk_field : 
        return 
    
    op = event["op"]
    row = event["after"] or event["before"]
    pk_value = row.get(pk_field)
    if pk_value is None:
        return
    
    if op == "d" :
        CACHE[table].pop(pk_value, None)
        CACHE_STATS['deletes'] += 1
    else: 
        was_present = pk_value in CACHE[table]
        CACHE[table][pk_value] = event["after"]
        if op == "c":
            CACHE_STATS["inserts"] += 1  # <-- ADD THIS
        elif op == "u":
            CACHE_STATS["updates"] += 1  # <-- ADD THIS

def cache_summary() -> str: 
    parts = [f"{table}={len(rows)}" for table, rows in CACHE.items()]
    stats_parts = " ".join([f"{k}={v}" for k, v in CACHE_STATS.items() if v > 0]) 
    return f"[cache] { " ".join(parts)} [stats] {stats_parts}"

def diff_fields(before: dict, after: dict):
    """Return only the fields that actually changed between before/after."""
    keys = set(before.keys()) | set(after.keys())
    changed = {}
    for k in keys:
        if before.get(k) != after.get(k):
            changed[k] = (before.get(k), after.get(k))
    return changed


def format_event(event: dict) -> str:
    table = event["table"]
    op = event["op"]
    label, color = OP_STYLE.get(op, (op, ""))
    before, after = event["before"], event["after"]

    pk_field = PK_FIELD.get(table)
    row = after or before
    pk_value = row.get(pk_field) if pk_field else "?"

    header = f"{color}[{label:<8}]{RESET} {table:<10} id={pk_value}"

    if op in ("c", "r"):
        # Show the new row compactly, skip noisy timestamp fields
        shown = {k: v for k, v in after.items() if k not in ("created_at", "updated_at")}
        return f"{header}  {shown}"

    if op == "d":
        return f"{header}  (row deleted)"

    if op == "u":
        changed = diff_fields(before, after)
        changed.pop("updated_at", None)  # always changes, not interesting to show
        if not changed:
            return f"{header}  (no visible field changes)"
        changes_str = ", ".join(f"{k}: {old!r} -> {new!r}" for k, (old, new) in changed.items())
        return f"{header}  {changes_str}"

    return f"{header}  {after}"

def query_loop() : 
    print("\nQuery the cache anytime: '<table> <id>' or 'summary'. Ctrl + c to quit. \n")

    while True: 
        try : 
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line : 
            continue

        if line == "summary" : 
            print(cache_summary())
            stats_str = " ".join([f"{k}={v}" for k, v in CACHE_STATS.items()])
            print(f"[cache-stats] {stats_str}")
            continue

        parts = line.split()
        if len(parts) != 2:
            print("Usage : <table> <id> e.g orders 43")
            CACHE_STATS["misses"] += 1
            continue

        table, id_str = parts
        if table not in CACHE: 
            print(f"unknown table '{table}'. Known table {list(CACHE.keys())}")
            CACHE_STATS["misses"]  += 1
            continue

        try : 
            pk_value = int(id_str)
        except ValueError: 
            pk_value = id_str

        row= CACHE[table].get(pk_value)
        if row is None: 
            print(f"no cached record for {table} id ={pk_value}")
            CACHE_STATS["misses"] += 1
        else: 
            print(f"{table} id = {pk_value}: {row}")
            CACHE_STATS["hits"] += 1
        
def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cdc_demo_consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    log.info(f"Listening for CDC events on: {TOPICS}\n")

    query_thread = threading.Thread(target=query_loop, daemon=True)
    query_thread.start()

    event_count = 0 

    for message in consumer:
        try : 
            if message.value is None:
                continue  # tombstone record, skip

            event = parse_event(message.value)
            if event is None:
                continue

            log.info(format_event(event))
            update_cache(event)

            event_count  += 1 
            if event_count % 10 == 0: 
                log.info(cache_summary())

        except Exception as e : 
            log.error(f"Error processing message: {e}")

if __name__ == "__main__":
    main()