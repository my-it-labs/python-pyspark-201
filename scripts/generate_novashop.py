#!/usr/bin/env python3
"""Genera el dataset sintético NovaShop (determinista) y escribe data/CANONICAL_COUNTS.json."""
from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"

COUNTRIES = ["ES", "FR", "PT", "DE", "IT"]
SEGMENTS = ["consumer", "corporate", "vip"]
CATEGORIES = ["electronics", "home", "fashion", "sports", "books"]
STATUSES = ["paid", "paid", "paid", "paid", "paid", "paid", "pending", "cancelled", "cancelled", "refunded"]
CHANNELS_RAW = ["web", "app", "store", "WEB", "App", "marketplace", "web", "app"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "search", "add_to_cart", "purchase"]


def main() -> None:
    rng = random.Random(SEED)
    RAW.mkdir(parents=True, exist_ok=True)

    customers = _customers(rng)
    products = _products(rng)
    orders = _orders(rng, customers)
    items = _items(rng, orders, products)
    events = _events(rng, customers, products, orders)
    profiles_v1, profiles_v2, profile_stats = _profiles(rng, customers)

    _write_customers(customers)
    _write_products(products)
    _write_orders(orders)
    _write_items(items)
    _write_events(events)
    _write_jsonl(RAW / "profiles_v1.jsonl", profiles_v1)
    _write_jsonl(RAW / "profiles_v2.jsonl", profiles_v2)

    counts = _canonical(customers, products, orders, items, events, profile_stats)
    out = ROOT / "data" / "CANONICAL_COUNTS.json"
    out.write_text(json.dumps(counts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(counts, indent=2, ensure_ascii=False))


def _customers(rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, 251):
        rows.append(
            {
                "customer_id": f"C{i:04d}",
                "full_name": f"Cliente {i:04d}",
                "country": rng.choice(COUNTRIES) if i > 5 else ("" if i <= 3 else None),
                "segment": rng.choice(SEGMENTS),
                "signup_date": (datetime(2023, 1, 1) + timedelta(days=rng.randint(0, 600))).strftime(
                    "%Y-%m-%d"
                ),
            }
        )
    return rows


def _products(rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, 61):
        price = round(rng.uniform(4.99, 299.99), 2)
        rows.append(
            {
                "productId": f"P{i:03d}",
                "name": f"Producto {i:03d}",
                "category": rng.choice(CATEGORIES),
                "listPrice": "" if i in {7, 19, 42} else f"{price:.2f}",
            }
        )
    return rows


def _orders(rng: random.Random, customers: list[dict]) -> list[dict]:
    ids = [c["customer_id"] for c in customers]
    rows = []
    for i in range(1, 801):
        ts = datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 364), hours=rng.randint(0, 23))
        customer_id = rng.choice(ids)
        if i <= 12:
            customer_id = ""
        elif i <= 20:
            customer_id = f"CX{i:03d}"  # huérfano
        date_val = ts.strftime("%Y-%m-%d %H:%M:%S")
        if i in {30, 31, 32}:
            date_val = ts.strftime("%d/%m/%Y")
        rows.append(
            {
                "OrderId": f"O{i:05d}",
                "CustomerId": customer_id,
                "OrderDate": date_val,
                "Status": STATUSES[(i + rng.randint(0, 3)) % len(STATUSES)],
                "Channel": CHANNELS_RAW[i % len(CHANNELS_RAW)],
            }
        )
    return rows


def _items(rng: random.Random, orders: list[dict], products: list[dict]) -> list[dict]:
    pids = [p["productId"] for p in products]
    rows = []
    line = 0
    for order in orders:
        n = rng.randint(1, 4)
        for _ in range(n):
            line += 1
            pid = rng.choice(pids)
            if line % 97 == 0:
                pid = ""
            elif line % 113 == 0:
                pid = "P999"
            qty = rng.randint(1, 5)
            if line % 131 == 0:
                qty = 0
            price = round(rng.uniform(4.99, 249.99), 2)
            discount = rng.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20])
            if line % 151 == 0:
                discount = 1.5
            rows.append(
                {
                    "order_id": order["OrderId"],
                    "product_id": pid,
                    "qty": qty,
                    "unit_price": f"{price:.2f}",
                    "discount": f"{discount:.2f}",
                }
            )
    return rows


def _events(
    rng: random.Random,
    customers: list[dict],
    products: list[dict],
    orders: list[dict],
) -> list[dict]:
    ids = [c["customer_id"] for c in customers]
    pids = [p["productId"] for p in products]
    rows = []
    for i in range(1, 2501):
        ts = datetime(2024, 1, 1) + timedelta(minutes=rng.randint(0, 364 * 24 * 60))
        cid = rng.choice(ids)
        if i <= 80:
            cid = None
        rows.append(
            {
                "event_id": f"E{i:06d}",
                "customer_id": cid,
                "event_type": rng.choice(EVENT_TYPES),
                "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "session_id": f"S{rng.randint(1, 900):04d}",
                "page": rng.choice(["/", "/catalog", "/product", "/cart", "/checkout"]),
                "product_id": rng.choice(pids + [None, None]),
            }
        )
    return rows


def _write_customers(rows: list[dict]) -> None:
    path = RAW / "customers.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["customer_id", "full_name", "country", "segment", "signup_date"])
        w.writeheader()
        for r in rows:
            w.writerow({**r, "country": "" if r["country"] is None else r["country"]})


def _write_products(rows: list[dict]) -> None:
    (RAW / "products.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_orders(rows: list[dict]) -> None:
    path = RAW / "orders.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["OrderId", "CustomerId", "OrderDate", "Status", "Channel"])
        w.writeheader()
        w.writerows(rows)


def _write_items(rows: list[dict]) -> None:
    path = RAW / "order_items.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["order_id", "product_id", "qty", "unit_price", "discount"])
        w.writeheader()
        w.writerows(rows)


def _write_events(rows: list[dict]) -> None:
    _write_jsonl(RAW / "events.jsonl", rows)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


_GEO = {
    "ES": (40.42, -3.70),
    "FR": (48.86, 2.35),
    "PT": (38.72, -9.14),
    "DE": (52.52, 13.40),
    "IT": (41.90, 12.50),
}
_CITY = {"ES": "Madrid", "FR": "Paris", "PT": "Lisboa", "DE": "Berlin", "IT": "Roma"}


def _profiles(rng: random.Random, customers: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """CRM en dos versiones: v1 plano (legacy) y v2 anidado. Solape a medias (migración incompleta)."""
    v1: list[dict] = []
    v2: list[dict] = []
    for c in customers:
        n = int(c["customer_id"][1:])
        country = c["country"] or ""
        if n <= 100:
            v1.append(
                {
                    "customer_id": c["customer_id"],
                    "fullName": c["full_name"],
                    "country": country,
                    "email": f"{c['customer_id'].lower()}@old.novashop.test",
                }
            )
        if n >= 51:
            geo = _GEO.get(country, _GEO["ES"])
            empty_preview = 74 <= n <= 79
            no_addr_country = 66 <= n <= 73
            email_null = 240 <= n <= 244
            n_prev = 0 if empty_preview else rng.randint(1, 3)
            preview = [
                {"id": f"X{n:04d}{k}", "gmv": round(rng.uniform(5, 90), 2)} for k in range(n_prev)
            ]
            v2.append(
                {
                    "customer_id": c["customer_id"],
                    "profile": {
                        "contact": {
                            "full_name": c["full_name"],
                            "email": {
                                "work": None if email_null else f"{c['customer_id'].lower()}@novashop.test",
                                "personal": None,
                            },
                            "address": {
                                "city": _CITY.get(country) if country else None,
                                "country": None if no_addr_country else (country or None),
                                "geo": {"lat": geo[0], "lon": geo[1]} if country else None,
                            },
                        },
                        "prefs": {"channel": rng.choice(["web", "app"]), "lang": "es"},
                        "country": country if no_addr_country else None,
                    },
                    "orders_preview": preview,
                    "meta": {"source": {"system": "crm", "version": 2}},
                }
            )
    stats = {
        "profiles_v1": len(v1),
        "profiles_v2": len(v2),
        "profiles_overlap": 50,
        "profiles_union": 250,
        "profiles_v2_empty_preview": 6,
        "profiles_v2_no_address_country": 8,
        "profiles_v2_email_work_null": 5,
    }
    return v1, v2, stats


def _canonical(customers, products, orders, items, events, profile_stats) -> dict:
    empty_country = sum(1 for c in customers if not c["country"])
    empty_cust_order = sum(1 for o in orders if not o["CustomerId"])
    orphan_cust_order = sum(1 for o in orders if str(o["CustomerId"]).startswith("CX"))
    bad_date = sum(1 for o in orders if "/" in o["OrderDate"])
    empty_pid = sum(1 for i in items if not i["product_id"])
    orphan_pid = sum(1 for i in items if i["product_id"] == "P999")
    qty_zero = sum(1 for i in items if int(i["qty"]) == 0)
    bad_discount = sum(1 for i in items if float(i["discount"]) > 1)
    events_no_cust = sum(1 for e in events if not e["customer_id"])
    products_no_price = sum(1 for p in products if p["listPrice"] == "")

    paid = sum(1 for o in orders if o["Status"] == "paid")
    return {
        "customers": len(customers),
        "customers_empty_country": empty_country,
        "products": len(products),
        "products_empty_list_price": products_no_price,
        "orders": len(orders),
        "orders_empty_customer_id": empty_cust_order,
        "orders_orphan_customer_id": orphan_cust_order,
        "orders_slash_date": bad_date,
        "orders_paid": paid,
        "order_items": len(items),
        "order_items_empty_product_id": empty_pid,
        "order_items_orphan_product_id": orphan_pid,
        "order_items_qty_zero": qty_zero,
        "order_items_discount_gt_1": bad_discount,
        "events": len(events),
        "events_null_customer_id": events_no_cust,
        **profile_stats,
        "m01_sample_paid": 3,
        "m02_orders_valid_customer_id": len(orders) - empty_cust_order,
        "fact_lines_after_order_inner": 1980,
        "fact_lines_dropped_orphan_orders": 30,
        "fact_lines_billable": 1127,
        "fact_lines_gmv_negative_before_cap": 13,
        "high_value_line_eur": 500.0,
        "high_value_lines_before_cap": 506,
    }


if __name__ == "__main__":
    main()
