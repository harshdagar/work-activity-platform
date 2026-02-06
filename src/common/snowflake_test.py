import os
from dotenv import load_dotenv, find_dotenv
import snowflake.connector

# Always find .env even if you run this script from a subfolder
load_dotenv(find_dotenv())

acct = os.getenv("SNOWFLAKE_ACCOUNT")
print("SNOWFLAKE_ACCOUNT =", acct)

conn = snowflake.connector.connect(
    account=acct,
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cur = conn.cursor()
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
print(cur.fetchone())
cur.close()
conn.close()

print("✅ Snowflake connection works")