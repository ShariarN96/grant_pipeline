"""Load NDJSON grant opportunities staged in ADLS into the Snowflake bronze table."""

import os
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv



@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    private_key_file: str
    warehouse: str = "COMPUTE_WH"
    database: str = "GRANTS"
    schema: str = "BRONZE"
    role: str = "ACCOUNTADMIN"

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        load_dotenv()
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        )


def load_opportunities_to_snowflake() -> list:
    """Run the COPY INTO statement that loads staged ADLS files into bronze_grants. Entry point for DAG tasks."""
    SQL_PATH = Path(__file__).parent / "test.sql"  # resolved relative to this file, not the cwd
    config = SnowflakeConfig.from_env()
    sql = SQL_PATH.read_text()

    conn = snowflake.connector.connect(
        account=config.account,
        user=config.user,
        private_key_file=config.private_key_file,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
        role=config.role,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        print(results)
        return results
    finally:
        conn.close()


if __name__ == "__main__":
    load_opportunities_to_snowflake()