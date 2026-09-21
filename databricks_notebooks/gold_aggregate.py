# Databricks notebook source
# 03 - Gold Aggregate
# fct_order_items: line-grain fact, promoted straight from
# silver.order_items_priced (already valid + priced - gold doesn't
# re-decide validity, silver already did).
#
# fct_orders: order-grain fact, joining orders_enriched with an
# order-level rollup of items, payments, and matched-only returns.
# loyalty_tier_at_purchase (point-in-time, via asof join against SCD2
# history) is intentionally NOT included yet - current_loyalty_tier
# from orders_enriched stands in for it until that join is built.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "retail_de"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


def write_gold(df, table_name):
    target = f"{GOLD}.{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {df.count()} rows")
    return target

# COMMAND ----------

# fct_order_items - straight promotion, line grain

order_items_priced = spark.table(f"{SILVER}.order_items_priced")
write_gold(order_items_priced, "fct_order_items")

# COMMAND ----------

# Order-level rollup of items - used to build fct_orders below.
# valid_item_count and order_net_amount both derive only from VALID
# lines (order_items_priced already excludes invalid ones), so an
# order with bad lines will show a lower total than its raw items
# would suggest - carried forward deliberately from silver.

item_agg = order_items_priced.groupBy("order_id").agg(
    F.count("*").alias("valid_item_count"),
    F.sum("gross_amount").alias("order_gross_amount"),
    F.sum("net_amount").alias("order_net_amount"),
)

# COMMAND ----------

# Payment info at order grain.
# The generator creates payments 1:1 with orders (payment_id ==
# order_id), so no aggregation needed here - just select the columns
# fct_orders wants.

payments_at_order_grain = spark.table(f"{SILVER}.payments_validated").select(
    "order_id",
    F.col("amount").alias("payment_amount"),
    "is_valid_settlement_sequence",
    "settlement_lag_days",
)

# COMMAND ----------

# Returns rollup - matched only (is_orphaned = false), per the
# original design: orphaned returns don't belong to any real order,
# so they can't be attributed to one here.

returns_agg = (
    spark.table(f"{SILVER}.returns_matched")
    .filter("is_orphaned = false")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("return_count"),
        F.sum("refund_amount").alias("total_refund_amount"),
    )
)

# COMMAND ----------

# fct_orders - orders_enriched + the three rollups above.
# Left joins throughout: not every order has items that survived
# validation, not every order necessarily has a payment row in this
# synthetic data, and most orders have zero returns - all of those
# are legitimate, not bugs, so nulls/zeros here are expected.

orders_enriched = spark.table(f"{SILVER}.orders_enriched")

fct_orders = (
    orders_enriched.join(item_agg, on="order_id", how="left")
    .join(payments_at_order_grain, on="order_id", how="left")
    .join(returns_agg, on="order_id", how="left")
    .withColumn("valid_item_count", F.coalesce(F.col("valid_item_count"), F.lit(0)))
    .withColumn("order_net_amount", F.coalesce(F.col("order_net_amount"), F.lit(0.0)))
    .withColumn("return_count", F.coalesce(F.col("return_count"), F.lit(0)))
    .withColumn("total_refund_amount", F.coalesce(F.col("total_refund_amount"), F.lit(0.0)))
)

write_gold(fct_orders, "fct_orders")

# COMMAND ----------

# Sanity checks - the two invariants worth watching:
#  1. no fct_orders row should have a negative order_net_amount
#  2. row count should equal the source orders count (left joins,
#     so joining shouldn't have dropped or duplicated any orders)

negative_total_count = fct_orders.filter("order_net_amount < 0").count()
print(f"fct_orders rows with negative order_net_amount: {negative_total_count}")

source_order_count = orders_enriched.count()
gold_order_count = fct_orders.count()
print(f"orders_enriched: {source_order_count}  fct_orders: {gold_order_count}  match: {source_order_count == gold_order_count}")

for t in ["fct_order_items", "fct_orders"]:
    cnt = spark.table(f"{GOLD}.{t}").count()
    print(f"{t:16s} {cnt:>6} rows")