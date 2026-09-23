# Databricks notebook source
# 02a - Silver SCD2 (customers, products)
#
# Compares this run's bronze snapshot against silver's currently-open
# row per key. Unchanged rows are left alone; changed/new rows close
# the old version (valid_to = new row's source updated_at, matching
# the old dbt snapshot's dbt_valid_to semantics) and insert a new one.


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
    # sha2 over the tracked columns only (ignores
    # _ingested_at etc. from bronze, which would falsely trigger a new
    # version every single run otherwise)
    return df.withColumn(
        "scd_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256)
    )


def initial_load(table_name, cfg, bronze_df):
    # initialise silver, all rows open
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

    # 1. Prepare snapshot, hash, target. 
    #  hash only the tracked columns in bronze snapshot
    bronze_df = with_hash(spark.table(f"{BRONZE_SCHEMA}.{table_name}"), cfg["tracked_cols"])
    if not spark.catalog.tableExists(target):
        initial_load(table_name, cfg, bronze_df)
        return
    
    # wires to the live silver table for updates
    delta_tbl = DeltaTable.forName(spark, target)


    # 2. Find out changes
    # select open rows from silver
    # rename key and hash to avoid collision with bronze_df columns (silver = cur)
    current_open = (
        delta_tbl.toDF()
        .filter("is_current = true")
        .select(F.col(key).alias("cur_key"), F.col("scd_hash").alias("cur_hash"))
    )
    # selecting only the key & hash from silver to compare with bronze:
    # is key new?
    # is hash different?

    # Find mismatched.
    # all bronze rows kept. silver rows with no match in bronze are dropped.
    changed_or_new = bronze_df.join(
        current_open, bronze_df[key] == current_open["cur_key"], "left"
    ).where("cur_key IS NULL OR cur_hash <> scd_hash")

    changed_only = changed_or_new.where("cur_key IS NOT NULL")  # existing keys, hash differs

    # 3. Update for changes
    # Step 1: close out old rows of anything that changed (merge + whenMatchedUpdate)
    (
        delta_tbl.alias("t")
        .merge(changed_only.alias("s"), f"t.{key} = s.{key} AND t.is_current = true")
        .whenMatchedUpdate(
            set={"valid_to": f"s.{ts_col}", "is_current": "false"}
        )
        .execute()
    )

    # Step 2: insert new open versions (append to target will be cheaper)
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


"""
Columns in silver SCD2 tables:

-- customers_scd2:

customer_id, first_name, last_name, email,        <- from CSV
signup_date_raw, loyalty_tier, updated_at_raw,    <- original strings renamed in place
signup_date, updated_at,                          <- parsed timestamps (bronze)
email_is_null, loyalty_tier_is_null,              <- null flags (bronze)
_ingested_at,                                     <- bronze
scd_hash,                                         <- with_hash()
valid_from, valid_to, is_current                  <- SCD2


-- products_scd2:
product_id, product_name, category, unit_price, updated_at_raw,
updated_at, _ingested_at, scd_hash, valid_from, valid_to, is_current
"""


