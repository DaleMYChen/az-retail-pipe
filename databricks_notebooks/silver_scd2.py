# Databricks notebook source
# 02a - Silver SCD2 (customers, products)
#
# Compares this run's bronze snapshot against silver's currently-open
# row per key. Unchanged rows are left alone; changed/new rows close
# the old version (valid_to = new row's source updated_at, matching
# the old dbt snapshot's dbt_valid_to semantics) and insert a new one.
#
# First run for a table: no silver table exists yet -> pure initial load,
# every bronze row becomes an open version (valid_from = updated_at,
# valid_to = null, is_current = true).

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "retail_de"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"

SCD2_CONFIG = {
    "customers": {
        "key": "customer_id",
        "tracked_cols": ["first_name", "last_name", "email", "loyalty_tier"],
        "source_ts_col": "updated_at",
    },
    "products": {
        "key": "product_id",
        "tracked_cols": ["product_name", "category", "unit_price"],
        "source_ts_col": "updated_at",
    },
}

# COMMAND ----------


def with_hash(df, tracked_cols):
    # sha2 over the tracked columns only - this is what "did anything
    # meaningful change" is decided on, not the whole row (ignores
    # _ingested_at etc. from bronze, which would falsely trigger a new
    # version every single run otherwise)
    return df.withColumn(
        "scd_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256)
    )


def initial_load(table_name, cfg, bronze_df):
    ts_col = cfg["source_ts_col"]
    target = f"{SILVER_SCHEMA}.{table_name}_scd2"

    out = (
        bronze_df.withColumn("valid_from", F.col(ts_col))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    out.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"{target}: initial load, {out.count()} rows")


def apply_scd2(table_name, cfg):
    key = cfg["key"]
    ts_col = cfg["source_ts_col"]
    target = f"{SILVER_SCHEMA}.{table_name}_scd2"

    bronze_df = with_hash(spark.table(f"{BRONZE_SCHEMA}.{table_name}"), cfg["tracked_cols"])

    if not spark.catalog.tableExists(target):
        initial_load(table_name, cfg, bronze_df)
        return

    delta_tbl = DeltaTable.forName(spark, target)

    # snapshot of currently-open rows, taken once and reused for both
    # steps below so step 1 and step 2 agree on what "changed" means
    current_open = (
        delta_tbl.toDF()
        .filter("is_current = true")
        .select(F.col(key).alias("cur_key"), F.col("scd_hash").alias("cur_hash"))
    )

    changed_or_new = bronze_df.join(
        current_open, bronze_df[key] == current_open["cur_key"], "left"
    ).where("cur_key IS NULL OR cur_hash <> scd_hash")

    changed_only = changed_or_new.where("cur_key IS NOT NULL")  # existing keys, hash differs

    # Step 1: close out the old version of anything that changed
    (
        delta_tbl.alias("t")
        .merge(changed_only.alias("s"), f"t.{key} = s.{key} AND t.is_current = true")
        .whenMatchedUpdate(
            set={"valid_to": f"s.{ts_col}", "is_current": "false"}
        )
        .execute()
    )

    # Step 2: insert new open versions - both brand-new keys and changed keys
    new_versions = (
        changed_or_new.select(bronze_df.columns)
        .withColumn("valid_from", F.col(ts_col))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    new_versions.write.format("delta").mode("append").saveAsTable(target)

    print(f"{target}: {new_versions.count()} row(s) versioned this run")

# COMMAND ----------

for table_name, cfg in SCD2_CONFIG.items():
    apply_scd2(table_name, cfg)

# COMMAND ----------

# Sanity check - every key should have exactly one is_current = true row
for table_name, cfg in SCD2_CONFIG.items():
    key = cfg["key"]
    target = f"{SILVER_SCHEMA}.{table_name}_scd2"
    dupes = (
        spark.table(target)
        .filter("is_current = true")
        .groupBy(key)
        .count()
        .filter("count > 1")
    )
    n_dupes = dupes.count()
    total_current = spark.table(target).filter("is_current = true").count()
    total_versions = spark.table(target).count()
    print(f"{table_name:10s} current={total_current:>5} total_versions={total_versions:>5} duplicate_current_keys={n_dupes}")