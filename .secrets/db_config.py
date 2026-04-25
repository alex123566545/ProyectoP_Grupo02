import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host="aws-1-us-east-2.pooler.supabase.com",
        port=5432,
        database="postgres",
        user="postgres.klbmaoqxfvjsczwrrwkj",
        password="alex20151615665451",
        sslmode="require"
    )