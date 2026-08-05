import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# THIS is the connection to Snowflake
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    warehouse="COMPUTE_WH",
    database="GRANTS",
    schema="BRONZE",
    role="ACCOUNTADMIN",
)

# read the .sql file (just loads the text into a string)
sql = open("test.sql").read()

# send that text to Snowflake THROUGH the connection
cur = conn.cursor()
cur.execute(sql)
print(cur.fetchall())
conn.close()