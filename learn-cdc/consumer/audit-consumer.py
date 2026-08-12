import json 
import logging 
import psycopg2
import sys

import signal, time

from kafka import KafkaConsumer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from cdc_common import BOOTSTRAP_SERVERS, TOPICS, PK_FIELD, DB_CONFIG, parse_event

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("audit_consumer")

AUDIT_METRICS = { 
    "written": 0,
    "errors": 0,
    "inserts": 0,
    "updates": 0,
    "deletes": 0,
    "snapshots": 0
}

SHUTDOWN = False

def write_audit_row(cur, event: dict): 
    table = event["table"]
    op  = event["op"]
    pk_field = PK_FIELD.get(table)

    if not pk_field : 
        return 

    row = event["after"] or event["before"]
    pk_value = row.get(pk_field)

    cur.execute( 
        """
        INSERT INTO cdc_audit (source_table, operation, pk_value, before_json, after_json)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            table,
            op,
            str(pk_value) if pk_value is not None else None,
            json.dumps(event["before"]) if event["before"] else None,
            json.dumps(event["after"]) if event["after"] else None,
        ),
    )
    AUDIT_METRICS["written"] += 1
    if op == "c":
        AUDIT_METRICS["inserts"] += 1
    elif op == "u":
        AUDIT_METRICS["updates"] += 1
    elif op == "d":
        AUDIT_METRICS["deletes"] += 1
    elif op == "r":
        AUDIT_METRICS["snapshots"] += 1

def signal_handler(signum, frame):
    """HANDLE SHUTDOWN SIGNAL GRACEFULLY"""
    log.info(f"Received signal {signum}, initiating graceful shutdown")
    global SHUTDOWN 
    SHUTDOWN = True

def get_db_connection(max_retries=3 ): 
    """Establish postgresql connectoin with exponential backoff retry"""
    for attempt in range(max_retries): 
        try: 
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True 
            log.info(f"Database connection established (attempt{ attempt + 1})")
            return conn 
        except psycopg2.OperationalError as e:
            log.warning(f"Database connection attempt {attempt + 1 } failed: {e}")
            if attempt < max_retries - 1  : 
                time.sleep(2 ** attempt )
            else : 
                log.error("Max connection retries exceeded")
                raise  

def main(): 
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    conn = None 
    cur = None

    while conn is None and not SHUTDOWN  : 
        try: 
            conn = get_db_connection() 
            cur = conn.cursor() 
        except Exception as e : 
            log.error(f"Failed to established database connection: {e}")
            if not SHUTDOWN: 
                log.info("Retrying connection in 5 second")
                time.sleep(5)
    if SHUTDOWN: 
        return 

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers = BOOTSTRAP_SERVERS,
        auto_offset_reset ="earliest", 
        enable_auto_commit = True,
        group_id = "cdc_audit_consumer", 
        value_deserializer = lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    log.info("[audit-consumer] started successfully")
    log.info(f"[audit-consumer] listening on: {TOPICS}\n")

    for message in consumer :
        if SHUTDOWN:  # Check for shutdown signal
            break

        if message.value is None :
            continue

        event = parse_event(message.value)
        if event is None:
            continue

        try:
            # Process the event
            write_audit_row(cur, event)
            log.info(f"[AUDIT] {event['table']} op= {event['op']} pk={event.get('after', {}).get(PK_FIELD.get(event['table'])) or event.get('before', {}).get(PK_FIELD.get(event['table']))}")

            # Log metrics every 100 written records
            if AUDIT_METRICS["written"] % 100 == 0:
                log.info(f"[AUDIT METRICS] {AUDIT_METRICS}")

        except psycopg2.OperationalError as e:
            # Handle database connection loss
            log.error(f"Database operational error: {e}")
            AUDIT_METRICS["errors"] += 1
          
            # Attempt to reconnect
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

            except Exception : 
                pass

            conn = None
            cur = None

            # Reconnect loop
            while not SHUTDOWN and conn is None:
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    log.info("Database reconnected successfully")
                except Exception as reconnect_e:
                    log.error(f"Reconnection attempt failed: {reconnect_e}")
                    if not SHUTDOWN:
                        time.sleep(5)
        except Exception as e:
            # Handle other unexpected errors
            log.error(f"Error processing audit event: {e}")
            AUDIT_METRICS["errors"] += 1
            # Continue processing other events
        
    log.info("shutting down audit consumer..")
    log.info(f"[FINAL AUDIT METRICS] {AUDIT_METRICS}")

    try:
        if cur:
            cur.close()
    except Exception as e:
        log.error(f"Error closing cursor: {e}")

    try:
        if conn:
            conn.close()
    except Exception as e:
        log.error(f"Error closing connection: {e}")

    try:
        if consumer:
            consumer.close()
    except Exception as e:
        log.error(f"Error closing consumer: {e}")
        log.info("[audit-consumer] Shutdown complete")

if __name__ == "__main__":
    main() 