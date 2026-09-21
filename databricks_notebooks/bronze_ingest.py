# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingest
# MAGIC Reads raw CSVs from the `retail_de.raw.landing` volume, applies light
# MAGIC type casting, flags (does not drop) nulls in key columns, and writes
# MAGIC each table as Delta into `retail_de.bronze`.
# MAGIC
# MAGIC This mirrors the intent of the old `stg_` layer: fix data-quality
# MAGIC issues without discarding anything, so downstream (silver) can decide
# MAGIC what "valid" means.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = "retail_de"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/landing"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config: which columns to null-flag, which to parse as datetimes
# MAGIC The source data mixes three date formats per the generator
# MAGIC (`%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`, `%d/%m/%Y`) — `parse_messy_datetime`
# MAGIC tries each in turn via `coalesce`, so whichever format matches wins.

TABLE_CONFIG = {
    "customers": {
        "null_flag_cols": ["email", "loyalty_tier"],
        "datetime_cols": ["signup_date", "updated_at"],
    },
    "products": {
        "null_flag_cols": [],
        "datetime_cols": ["updated_at"],
    },
    "stores": {
        "null_flag_cols": [],
        "datetime_cols": [],
    },
    "orders": {
        "null_flag_cols": [],
        "datetime_cols": ["order_date"],
    },
    "order_items": {
        "null_flag_cols": [],
        "datetime_cols": [],
    },
    "payments": {
        "null_flag_cols": [],
        "datetime_cols": ["payment_date", "settle_date"],
    },
    "returns": {
        "null_flag_cols": [],
        "datetime_cols": ["return_date"],
    },
}

DATE_FORMATS = ["yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy"]

# COMMAND ----------


def parse_messy_datetime(col_name: str):
    """Try each known source format in turn; first successful parse wins.
    Rows that match none of them come through as null in the cast column
    (visible for silver-layer validation), original string is preserved."""
    attempts = [F.try_to_timestamp(F.col(col_name), F.lit(fmt)) for fmt in DATE_FORMATS]
    return F.coalesce(*attempts)


def add_null_flags(df: DataFrame, cols: list[str]) -> DataFrame:
    for c in cols:
        df = df.withColumn(f"{c}_is_null", F.col(c).isNull())
    return df


def ingest_table(table_name: str, cfg: dict) -> DataFrame:
    raw_path = f"{VOLUME_PATH}/{table_name}.csv"
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    # Cast messy datetime strings, keep original alongside as *_raw for audit
    for date_col in cfg["datetime_cols"]:
        df = (
            df.withColumnRenamed(date_col, f"{date_col}_raw")
            .withColumn(date_col, parse_messy_datetime(f"{date_col}_raw"))
        )

    df = add_null_flags(df, cfg["null_flag_cols"])
    df = df.withColumn("_ingested_at", F.current_timestamp())
    return df

# COMMAND ----------

# MAGIC %md ### Run for every table, write Delta, register in `retail_de.bronze`

for table_name, cfg in TABLE_CONFIG.items():
    bronze_df = ingest_table(table_name, cfg)
    target = f"{BRONZE_SCHEMA}.{table_name}"

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    row_count = bronze_df.count()
    print(f"{target}: {row_count} rows written")

# COMMAND ----------

# MAGIC %md ### Quick sanity check
# MAGIC Confirms row counts and shows a peek at the messiest table
# MAGIC (`payments`, which carries two parsed datetime columns).

display(spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.payments LIMIT 10"))

# COMMAND ----------

for table_name in TABLE_CONFIG:
    cnt = spark.table(f"{BRONZE_SCHEMA}.{table_name}").count()
    print(f"{table_name:15s} {cnt:>6} rows")