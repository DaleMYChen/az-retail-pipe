# Databricks notebook source
# 02b - Silver Transform
# The non-SCD2 silver tables: validation, pricing, enrichment, and
# orphan-detection logic, mirroring the old int_ layer's five models.
# Everything here reads from bronze; order_items_priced and
# orders_enriched additionally read the SCD2 tables from 02a for
# current-state customer/product attributes.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "retail_de"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"


def write_silver(df, table_name):
    target = f"{SILVER}.{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {df.count()} rows")
    return target

# COMMAND ----------

# order_items_validated
# A line is valid if: quantity > 0, unit_price > 0, and its product_id
# exists in the current product dimension (not an orphaned FK). Every
# row is kept - invalid ones are flagged, not dropped, so silver stays
# an honest record of what bronze actually contained.

order_items = spark.table(f"{BRONZE}.order_items")
current_products = spark.table(f"{SILVER}.products_scd2").filter("is_current = true").select("product_id")

order_items_validated = (
    order_items.withColumn("is_negative_quantity", F.col("quantity") <= 0)
    .withColumn("is_negative_price", F.col("unit_price") <= 0)
    .join(
        current_products.withColumn("_product_exists", F.lit(True)),
        on="product_id",
        how="left",
    )
    .withColumn("is_orphaned_product", F.col("_product_exists").isNull())
    .drop("_product_exists")
    .withColumn(
        "is_valid",
        ~F.col("is_negative_quantity") & ~F.col("is_negative_price") & ~F.col("is_orphaned_product"),
    )
)

write_silver(order_items_validated, "order_items_validated")

invalid_count = order_items_validated.filter("is_valid = false").count()
total_count = order_items_validated.count()
print(f"order_items_validated: {invalid_count}/{total_count} lines flagged invalid")

# COMMAND ----------

# order_items_priced
# Built on VALID lines only - this is the point the original README
# flagged deliberately: a "bad" order's total understates its true
# value, on purpose, because invalid lines are excluded here.

order_items_priced = (
    order_items_validated.filter("is_valid = true")
    .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("net_amount", F.col("gross_amount") * (F.lit(1) - F.col("discount")))
)

write_silver(order_items_priced, "order_items_priced")

# COMMAND ----------

# orders_enriched
# Joins orders to the CURRENT customer dimension (is_current = true)
# and to stores. This is current-state enrichment only - a
# point-in-time ("loyalty tier at time of purchase") version is a
# later enhancement once we add an asof join against SCD2 history.

orders = spark.table(f"{BRONZE}.orders")
current_customers = (
    spark.table(f"{SILVER}.customers_scd2")
    .filter("is_current = true")
    .select(
        "customer_id",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
        F.col("loyalty_tier").alias("current_loyalty_tier"),
    )
)
stores = spark.table(f"{BRONZE}.stores").select(
    "store_id", "store_name", "region", F.col("channel").alias("store_channel")
)

orders_enriched = (
    orders.join(current_customers, on="customer_id", how="left")
    .join(stores, on="store_id", how="left")
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "current_loyalty_tier",
        "store_id",
        "store_name",
        "region",
        "order_date",
        "status",
        F.col("channel").alias("order_channel"),
        "store_channel",
    )
)

write_silver(orders_enriched, "orders_enriched")

# COMMAND ----------

# payments_validated
# Flags settlement-before-payment (the deliberately-injected business
# logic bug) and computes settlement lag in days.

payments = spark.table(f"{BRONZE}.payments")

payments_validated = (
    payments.withColumn(
        "is_valid_settlement_sequence", F.col("settle_date") >= F.col("payment_date")
    )
    .withColumn(
        "settlement_lag_days",
        F.datediff(F.col("settle_date"), F.col("payment_date")),
    )
)

write_silver(payments_validated, "payments_validated")

bad_sequence_count = payments_validated.filter("is_valid_settlement_sequence = false").count()
print(f"payments_validated: {bad_sequence_count} rows with settlement before payment")

# COMMAND ----------

# returns_matched
# Left join to orders to catch orphaned returns (order_id with no
# matching order). Both matched and orphaned rows are kept here;
# gold decides whether to filter to matched-only.

returns = spark.table(f"{BRONZE}.returns")
orders_keys = orders.select(F.col("order_id").alias("_order_id"))

returns_matched = returns.join(
    orders_keys, returns["order_id"] == orders_keys["_order_id"], "left"
).withColumn("is_orphaned", F.col("_order_id").isNull()).drop("_order_id")

write_silver(returns_matched, "returns_matched")

orphaned_count = returns_matched.filter("is_orphaned = true").count()
total_returns = returns_matched.count()
print(f"returns_matched: {orphaned_count}/{total_returns} returns orphaned")

# COMMAND ----------

# Sanity check summary across all five silver tables

for t in ["order_items_validated", "order_items_priced", "orders_enriched", "payments_validated", "returns_matched"]:
    cnt = spark.table(f"{SILVER}.{t}").count()
    print(f"{t:22s} {cnt:>6} rows")