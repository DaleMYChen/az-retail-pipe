"""
Generate Faker-based retail CSVs with deliberate data-quality and
business-logic noise, for landing in ADLS Gen2 /raw.

Tables: customers, products, stores, orders, order_items, payments, returns

Noise injected (kept explicit/visible so downstream bronze/silver notebooks
have something real to clean):
  1. Data quality: nulls in optional fields, inconsistent datetime string formats
  2. Business logic: settlement before payment, negative amounts/quantities,
     orphaned foreign keys (returns -> orders, order_items -> products)

Usage:
    python generate_raw_data.py --out-dir ../data --seed 42
"""

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"]


def messy_datetime(dt: datetime) -> str:
    """Serialize a datetime using a randomly chosen format, to mimic
    inconsistent source-system exports."""
    fmt = random.choice(DATE_FORMATS)
    return dt.strftime(fmt)


def maybe_null(value, null_rate=0.05):
    return None if random.random() < null_rate else value


def gen_customers(fake: Faker, n: int, base_date: datetime) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        signup = base_date - timedelta(days=random.randint(30, 900))
        rows.append(
            {
                "customer_id": i,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": maybe_null(fake.email(), null_rate=0.03),
                "signup_date": messy_datetime(signup),
                "loyalty_tier": maybe_null(
                    random.choice(["bronze", "silver", "gold", "platinum"]),
                    null_rate=0.08,
                ),
                "updated_at": messy_datetime(base_date),
            }
        )
    return pd.DataFrame(rows)


def gen_products(fake: Faker, n: int, base_date: datetime) -> pd.DataFrame:
    categories = ["apparel", "electronics", "home", "beauty", "grocery", "toys"]
    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "product_id": i,
                "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
                "category": random.choice(categories),
                "unit_price": round(random.uniform(3, 250), 2),
                "updated_at": messy_datetime(base_date),
            }
        )
    return pd.DataFrame(rows)


def gen_stores(fake: Faker, n: int) -> pd.DataFrame:
    regions = ["NSW", "VIC", "QLD", "WA", "SA"]
    channels = ["in_store", "online"]
    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "store_id": i,
                "store_name": fake.city() + " Store",
                "region": random.choice(regions),
                "channel": random.choice(channels),
            }
        )
    return pd.DataFrame(rows)


def gen_orders(n: int, n_customers: int, n_stores: int, base_date: datetime) -> pd.DataFrame:
    statuses = ["completed", "cancelled", "pending"]
    rows = []
    for i in range(1, n + 1):
        order_date = base_date - timedelta(days=random.randint(0, 180))
        rows.append(
            {
                "order_id": i,
                "customer_id": random.randint(1, n_customers),
                "store_id": random.randint(1, n_stores),
                "order_date": messy_datetime(order_date),
                "status": random.choice(statuses),
                "channel": random.choice(["in_store", "online"]),
            }
        )
    return pd.DataFrame(rows)


def gen_order_items(n: int, n_orders: int, n_products: int, orphan_rate=0.02) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(3, 250), 2)
        # business-logic noise: occasional negative quantity/price
        if random.random() < 0.02:
            quantity = -quantity
        if random.random() < 0.02:
            unit_price = -unit_price
        product_id = (
            n_products + 999  # deliberately orphaned FK
            if random.random() < orphan_rate
            else random.randint(1, n_products)
        )
        rows.append(
            {
                "order_item_id": i,
                "order_id": random.randint(1, n_orders),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": round(random.uniform(0, 0.3), 2) if random.random() < 0.2 else 0.0,
            }
        )
    return pd.DataFrame(rows)


def gen_payments(n_orders: int, base_date: datetime, bad_sequence_rate=0.05) -> pd.DataFrame:
    rows = []
    for order_id in range(1, n_orders + 1):
        payment_date = base_date - timedelta(days=random.randint(0, 180))
        settle_delay = random.randint(1, 5)
        settle_date = payment_date + timedelta(days=settle_delay)
        # business-logic noise: settlement before payment
        if random.random() < bad_sequence_rate:
            settle_date = payment_date - timedelta(days=random.randint(1, 3))
        amount = round(random.uniform(10, 800), 2)
        if random.random() < 0.02:
            amount = -amount  # negative amount noise
        rows.append(
            {
                "payment_id": order_id,
                "order_id": order_id,
                "payment_date": messy_datetime(payment_date),
                "settle_date": messy_datetime(settle_date),
                "amount": amount,
                "payment_method": random.choice(["card", "paypal", "gift_card", "bank_transfer"]),
            }
        )
    return pd.DataFrame(rows)


def gen_returns(n: int, n_orders: int, base_date: datetime, orphan_rate=0.03) -> pd.DataFrame:
    reasons = ["defective", "wrong_item", "changed_mind", "damaged_in_transit"]
    rows = []
    for i in range(1, n + 1):
        return_date = base_date - timedelta(days=random.randint(0, 170))
        order_id = (
            n_orders + 999  # deliberately orphaned FK -> no matching order
            if random.random() < orphan_rate
            else random.randint(1, n_orders)
        )
        rows.append(
            {
                "return_id": i,
                "order_id": order_id,
                "return_date": messy_datetime(return_date),
                "refund_amount": round(random.uniform(5, 400), 2),
                "reason": random.choice(reasons),
            }
        )
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="../data", help="Directory to write CSVs into")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-customers", type=int, default=500)
    parser.add_argument("--n-products", type=int, default=150)
    parser.add_argument("--n-stores", type=int, default=12)
    parser.add_argument("--n-orders", type=int, default=3000)
    parser.add_argument("--n-order-items", type=int, default=7000)
    parser.add_argument("--n-returns", type=int, default=250)
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)
    fake = Faker()
    base_date = datetime(2026, 9, 1)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "customers": gen_customers(fake, args.n_customers, base_date),
        "products": gen_products(fake, args.n_products, base_date),
        "stores": gen_stores(fake, args.n_stores),
        "orders": gen_orders(args.n_orders, args.n_customers, args.n_stores, base_date),
        "order_items": gen_order_items(args.n_order_items, args.n_orders, args.n_products),
        "payments": gen_payments(args.n_orders, base_date),
        "returns": gen_returns(args.n_returns, args.n_orders, base_date),
    }

    for name, df in tables.items():
        path = out_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"wrote {len(df):>6} rows -> {path}")


if __name__ == "__main__":
    main()