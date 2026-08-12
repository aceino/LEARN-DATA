import sys
import random 
import time 
import logging 
import psycopg2

from faker import Faker
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from cdc_common import DB_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Faker_producer")

fake = Faker()

# How often to run a cycle of writes
SLEEP_SECONDS = 2

# Relative chance of each operation type per cycle
OP_WEIGHTS = {
    "insert_customer": 0.3,
    "insert_order": 0.35,
    "update": 0.25,
    "delete": 0.10,
}
 
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]

def get_connection(max_retries = 3):
    for attempt in range(max_retries): 
        try: 
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True 
            log.info(f"Database connection established (attempt {attempt + 1})")
            return conn 
        except psycopg2.OperationalError as e:
            log.warning(f"Connection attempt {attempt + 1} failed: {e}") 
            if attempt < max_retries - 1 : 
                time.sleep(2 ** attempt) 
            else : 
                log.error("Max connection retries exceeded")
                raise

def insert_customer(cur) :
    cur.execute(
        """
        insert into customers(first_name, last_name, email, address)
        values( %s, %s, %s, %s)
        returning customer_id
        """,
        (
            fake.first_name(),
            fake.last_name(), 
            fake.unique.email(), 
            fake.address(),
        )
    )
 
    customer_id = cur.fetchone()[0]
    log.info(f"insert customer_id ={customer_id}")
    return customer_id 

def insert_order(cur) :    
    # For PRODUCTS 
    cur.execute ("select product_id, unit_price from products order by random() limit 1")
    
    product_row = cur.fetchone() 
    if not product_row : 
        return     
    
    product_id, unit_price = product_row
    
    quantity = random.randint(1, 5) # RANDOM QUANTITY
    
    cur.execute ("select customer_id from customers order by random() Limit 1")

    customer_row= cur.fetchone()
    if not customer_row :
        return 
    
    customer_id = customer_row[0]

    cur.execute ( 
        """
        insert into orders (customer_id, product_id, total_amount, status)
        values (%s, %s, %s, %s )
        returning order_id
        """, 
        (customer_id,   
         product_id,
         float (unit_price * quantity),
         random.choice(ORDER_STATUSES)
        ),
    )

    order_id = cur.fetchone()[0]
    log.info(f"insert order_id ={order_id} customer_id={customer_id}")

def update_random_row(cur):
    table = random.choice(["customers", "orders"])

    if table == "customers": 
        cur.execute("select customer_id from customers order by random() limit 1")
        row = cur.fetchone()
        if not row : 
            return 
        
        cur.execute(
            """
            update customers set address = %s, updated_at= now() where customer_id = %s
            """,
            (fake.address(), row[0])
        )
        log.info(f"update customer_id={row[0]}")
    else:
        cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        cur.execute(
            """
            UPDATE orders
            SET status = %s, updated_at = now()
            WHERE order_id = %s
            """,
            (random.choice(ORDER_STATUSES), row[0]),
        )
        log.info(f"UPDATE order id={row[0]}")
 
def delete_random_row(cur):
    # Delete orders more often than customers to avoid FK issues / running out of data
    table = random.choices(["orders", "customers"], weights=[0.85, 0.15])[0]
 
    if table == "orders":
        cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
        log.info(f"DELETE order id={row[0]}")
    else:
        # only delete a customer if they have no orders left, to respect the FK
        cur.execute(
            """
            SELECT c.customer_id FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.customer_id
            WHERE o.order_id IS NULL
            ORDER BY random() LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return
        cur.execute("DELETE FROM customers WHERE customer_id = %s", (row[0],))
        log.info(f"DELETE customer id={row[0]}")

def insert_product(cur): 
    cur.execute(
        """
        INSERT INTO products (sku, name, description, unit_price)
        VALUES (%s, %s,%s, %s)
        RETURNING product_id
        """,
        (
            fake.unique.bothify(text="SKU-####-????").upper(),
            fake.word().capitalize(),
            fake.sentence(nb_words=6),
            round(random.uniform(5.0, 500.0), 2),
        ),
    )
    product_id = cur.fetchone()[0]
    log.info(f"insert product_id={product_id}")
    return product_id

def run_cycle(cur):
    ops = list(OP_WEIGHTS.keys())
    weights = list(OP_WEIGHTS.values())
    op = random.choices(ops, weights=weights)[0]
 
    try:
        if op == "insert_customer":
            insert_customer(cur)
        elif op == "insert_order":
            insert_order(cur)
        elif op == "update":
            update_random_row(cur)
        elif op == "delete":
            delete_random_row(cur)
    except Exception as e:
        log.error(f"Error during {op}: {e}")

def main():
    log.info("Starting Faker CDC producer...")
    conn = None
    cur = None

    try : 
        conn = get_connection()
        cur = conn.cursor() 

        # for products
        for _ in range (10 ): 
            insert_order(cur)

        # seed a handful of customers so updates/deletes/orders have something to work with
        for _ in range(10):
            insert_customer(cur)

        while True:
            try:
                run_cycle(cur)
            except psycopg2.OperationalError as e : 
                log.error(f"Database connection lost : {e}")
                log.info("Attempting to reconnect")

                if cur: 
                    cur.close()
                if conn : 
                    conn.close()

                conn = get_connection()
                cur = conn.cursor() 
                log.info("Reconnect Sucessfully")

            except Exception as e : 
                log.error(f"Unexpected error during cycle {e}")
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt : 
        log.info("Stopping producer")

    except Exception as e : 
        log.error(f"Fatal error in producer {e}")
    finally :
        if cur : 
            cur.close()
        if conn: 
            conn.close()

 
if __name__ == "__main__":
    main()