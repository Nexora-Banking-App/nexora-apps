from fastapi import FastAPI, HTTPException, Header, Query
from prometheus_fastapi_instrumentator import Instrumentator
from dbutils.pooled_db import PooledDB
import pymysql
import os

app = FastAPI()
Instrumentator().instrument(app).expose(app)

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=15,
    mincached=0,  # Lazy initialization
    maxcached=5,
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root"),
    database=os.getenv("DB_NAME", "nexora_bank"),
    cursorclass=pymysql.cursors.DictCursor
)

@app.get("/health/liveness")
def liveness(): return {"status": "alive"}

@app.get("/health/readiness")
def readiness():
    try:
        conn = db_pool.connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database unready")

@app.get("/account/me")
def get_account_details(
    x_user_id: int = Header(..., alias="X-User-Id"),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    conn = db_pool.connection()
    try:
        with conn.cursor() as cursor:
            # Strictly Read-Only Query (No money creation, no side effects)
            cursor.execute("""
                SELECT u.username, a.balance 
                FROM users u 
                JOIN accounts a ON u.id = a.user_id 
                WHERE u.id = %s
            """, (x_user_id,))
            acc = cursor.fetchone()
            
            if not acc:
                raise HTTPException(status_code=404, detail="Account ledger record not provisioned")

            # Paginated Transaction History Query
            cursor.execute("""
                SELECT t.id, t.idempotency_key, t.amount, t.timestamp, 
                       sender.username as sender_name, 
                       receiver.username as receiver_name
                FROM transactions t
                JOIN users sender ON t.sender_id = sender.id
                JOIN users receiver ON t.receiver_id = receiver.id
                WHERE t.sender_id = %s OR t.receiver_id = %s 
                ORDER BY t.timestamp DESC
                LIMIT %s OFFSET %s
            """, (x_user_id, x_user_id, limit, offset))
            txs = cursor.fetchall()

        for tx in txs:
            tx["amount"] = str(tx["amount"])
            tx["timestamp"] = tx["timestamp"].strftime("%Y-%m-%d %H:%M:%S")

        return {
            "username": acc["username"], 
            "balance": str(acc["balance"]), 
            "transactions": txs,
            "page_limit": limit,
            "page_offset": offset
        }
    finally:
        conn.close()