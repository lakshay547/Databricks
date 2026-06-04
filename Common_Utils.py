# Databricks notebook source
staging_catalog= "etl_accord_us2"
staging_schema= "staging"
source_mysql_user = "confluent_user"
source_mysql_password = "Ribv92rtAv*9tbvAVna"
source_mysql_server = "accord-usprod2-mysql-db.mysql.database.azure.com"
source_postgres_server = "psql-accord-usprod2-db.postgres.database.azure.com"
source_postgres_user = "psql_accord_usprod2_admin"
source_postgres_password = "Fab9o3vnabna214bcsa"
port = "3306"
postgres_port = "5432"
custom_sql = "None"
# jdbc_driver="com.mysql.cj.jdbc.Driver"
jdbc_properties = {
    "user": source_mysql_user,
    "password": source_mysql_password
}

# COMMAND ----------

def log(msg: str):
    from datetime import datetime
    ts = datetime.now().replace(microsecond=0)
    print(f"{ts}: {msg}")

# COMMAND ----------

import json

def load_and_validate_table_config(config_path: str) -> list[dict]:
    """
    Load MySQL table configuration JSON and validate required keys.
    """

    REQUIRED_KEYS = {
        "source_schema",
        "source_table",
        "primary_column",
        "partition_column",
        "target_catalog",
        "target_schema",
        "target_table"
    }

    with open(config_path, "r") as f:
        tables = json.load(f)

    if not isinstance(tables, list):
        raise ValueError("Config file must contain a list of table definitions")

    for idx, t in enumerate(tables, start=1):
        if not isinstance(t, dict):
            raise ValueError(f"Table config at index {idx} is not a dictionary")

        missing = REQUIRED_KEYS - t.keys()
        if missing:
            raise ValueError(
                f"Missing keys {missing} in table config at index {idx}: {t}"
            )

    return tables



# COMMAND ----------

def should_load_table(table_cfg: dict, load_type: str) -> bool:
    """
    Decides whether a table should be loaded based on load_type.
    """

    if not load_type:
        return True

    lt = load_type.strip().lower()

    # 1️⃣ ALL → load everything
    if lt == "all":
        return True

    # 2️⃣ Module-based load
    module = table_cfg.get("module", "").lower()
    if lt == module:
        return True

    # 3️⃣ Explicit table list
    table_list = {t.strip().lower() for t in lt.split(",")}
    print(table_list)
    if table_cfg["target_table"].lower() in table_list:
        return True

    return False
