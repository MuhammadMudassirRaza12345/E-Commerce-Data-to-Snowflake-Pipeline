"""
extract_and_load.py
===================
E-Commerce Data Pipeline
  1. Parallel API extraction  (ThreadPoolExecutor)
  2. Pandas DataFrames        (transform + schema detect)
  3. Snowflake auto-setup     (creates schemas + tables automatically)
  4. Bulk upload              (write_pandas → auto schema detect like BQ)

Usage:
    pip install requests pandas snowflake-connector-python
                "snowflake-connector-python[pandas]" python-dotenv
    python extract_and_load.py
"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict
 
...
# Snowflake imports
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─────────────────────────────────────────────
# 0.  CONFIG
# ─────────────────────────────────────────────
load_dotenv()

BASE_URL   = "https://dummyjson.com"
PAGE_SIZE  = 100
EXTRACTED_AT = datetime.now(timezone.utc).isoformat()

# Snowflake connection config from .env
SF_CONFIG = {
    "account"  : os.getenv("SNOWFLAKE_ACCOUNT"),
    "user"     : os.getenv("SNOWFLAKE_USER"),
    "password" : os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database" : os.getenv("SNOWFLAKE_DATABASE",  "ECOMMERCE_DB"),
    "role"     : os.getenv("SNOWFLAKE_ROLE",       "ACCOUNTADMIN"),
}

# What we extract → which Snowflake schema + table
ENDPOINTS = {
    "products"  : {"path": "/products",  "schema": "RAW", "table": "RAW_PRODUCTS"},
    "users"     : {"path": "/users",     "schema": "RAW", "table": "RAW_USERS"},
    "carts"     : {"path": "/carts",     "schema": "RAW", "table": "RAW_CARTS"},
    "cart_items": {"path": "/carts",     "schema": "RAW", "table": "RAW_CART_ITEMS"},
}

# ─────────────────────────────────────────────
# 1.  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 2.  PARALLEL API EXTRACTION
# ─────────────────────────────────────────────
def fetch_page(session: requests.Session, url: str, skip: int) -> dict:
    """Fetch one page with retry logic (3 attempts)."""
    for attempt in range(1, 4):
        try:
            resp = session.get(
                url,
                params={"limit": PAGE_SIZE, "skip": skip},
                timeout=15
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.warning("Attempt %d failed → %s : %s", attempt, url, e)
            if attempt == 3:
                raise
    return {}


def fetch_all_pages(endpoint_name: str, path: str) -> List[Dict]:
    """
    Parallel pagination using ThreadPoolExecutor.
    
    Strategy:
      1. Fetch first page  → get total count
      2. Calculate remaining pages
      3. Fetch all pages IN PARALLEL
      4. Merge and return
    """
    url     = BASE_URL + path
    session = requests.Session()
    session.headers.update({
        "Accept"    : "application/json",
        "User-Agent": "Mozilla/5.0 (EcommercePipeline/1.0)",
    })

    log.info("[%s] Starting extraction → %s", endpoint_name, url)

    # ── First page to know total count
    first_page = fetch_page(session, url, skip=0)
    total      = first_page.get("total", 0)
    data_key   = _get_data_key(first_page)
    records    = first_page.get(data_key, [])

    log.info("[%s] Total records to fetch: %d", endpoint_name, total)

    # ── Calculate remaining skips
    skips = list(range(PAGE_SIZE, total, PAGE_SIZE))

    if skips:
        # ── Fetch remaining pages IN PARALLEL
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(fetch_page, session, url, skip): skip
                for skip in skips
            }
            for future in as_completed(futures):
                skip = futures[future]
                try:
                    page     = future.result()
                    batch    = page.get(data_key, [])
                    records.extend(batch)
                    log.info(
                        "[%s] Fetched skip=%-4d | running total: %d / %d",
                        endpoint_name, skip, len(records), total
                    )
                except Exception as e:
                    log.error("[%s] Failed page skip=%d: %s", endpoint_name, skip, e)

    log.info("[%s] ✅ Extraction complete → %d records", endpoint_name, len(records))
    return records


def _get_data_key(page: dict) -> str:
    """Detect the list key in the response (products / users / carts)."""
    for key in ["products", "users", "carts"]:
        if key in page:
            return key
    return ""


# ─────────────────────────────────────────────
# 3.  PANDAS TRANSFORMS  (+ schema auto-detect)
# ─────────────────────────────────────────────
def build_products_df(records: List[Dict]) -> pd.DataFrame:
    """
    Flatten and type-cast products.
    
    Raw shape:
        id, title, description, price, discountPercentage,
        rating, stock, brand, category, thumbnail
    """
    rows = []
    for p in records:
        rows.append({
            "PRODUCT_ID"          : p.get("id"),
            "TITLE"               : p.get("title"),
            "DESCRIPTION"         : p.get("description"),
            "PRICE"               : p.get("price"),
            "DISCOUNT_PERCENTAGE" : p.get("discountPercentage"),
            "RATING"              : p.get("rating"),
            "STOCK"               : p.get("stock"),
            "BRAND"               : p.get("brand"),
            "CATEGORY"            : p.get("category"),
            "THUMBNAIL_URL"       : p.get("thumbnail"),
            "EXTRACTED_AT"        : EXTRACTED_AT,
        })

    df = pd.DataFrame(rows)

    # ── Type casting (schema auto-detect reads these dtypes)
    df["PRODUCT_ID"]          = df["PRODUCT_ID"].astype("Int64")
    df["PRICE"]               = df["PRICE"].astype(float)
    df["DISCOUNT_PERCENTAGE"] = df["DISCOUNT_PERCENTAGE"].astype(float)
    df["RATING"]              = df["RATING"].astype(float)
    df["STOCK"]               = df["STOCK"].astype("Int64")
    df["EXTRACTED_AT"]        = pd.to_datetime(df["EXTRACTED_AT"], utc=True)

    log.info("Products DataFrame → shape: %s", df.shape)
    log.info("Products schema:\n%s", df.dtypes.to_string())
    return df


def build_users_df(records: List[Dict]) -> pd.DataFrame:
    """
    Flatten nested address + company fields.
    
    Raw shape:
        id, firstName, lastName, email, phone, gender, age,
        address: {city, country}, company: {name}
    """
    rows = []
    for u in records:
        address = u.get("address", {})
        company = u.get("company", {})
        rows.append({
            "USER_ID"      : u.get("id"),
            "FIRST_NAME"   : u.get("firstName"),
            "LAST_NAME"    : u.get("lastName"),
            "EMAIL"        : u.get("email"),
            "PHONE"        : u.get("phone"),
            "GENDER"       : u.get("gender"),
            "AGE"          : u.get("age"),
            "CITY"         : address.get("city"),
            "COUNTRY"      : address.get("country"),
            "COMPANY"      : company.get("name"),
            "EXTRACTED_AT" : EXTRACTED_AT,
        })

    df = pd.DataFrame(rows)

    # ── Type casting
    df["USER_ID"]     = df["USER_ID"].astype("Int64")
    df["AGE"]         = df["AGE"].astype("Int64")
    df["EXTRACTED_AT"]= pd.to_datetime(df["EXTRACTED_AT"], utc=True)

    log.info("Users DataFrame → shape: %s", df.shape)
    log.info("Users schema:\n%s", df.dtypes.to_string())
    return df


def build_carts_df(records: List[Dict]) -> pd.DataFrame:
    """
    Cart header rows (without nested products[]).
    
    Raw shape:
        id, userId, total, discountedTotal,
        totalProducts, totalQuantity
    """
    rows = []
    for c in records:
        rows.append({
            "CART_ID"         : c.get("id"),
            "USER_ID"         : c.get("userId"),
            "TOTAL"           : c.get("total"),
            "DISCOUNTED_TOTAL": c.get("discountedTotal"),
            "TOTAL_PRODUCTS"  : c.get("totalProducts"),
            "TOTAL_QUANTITY"  : c.get("totalQuantity"),
            "EXTRACTED_AT"    : EXTRACTED_AT,
        })

    df = pd.DataFrame(rows)

    # ── Type casting
    df["CART_ID"]         = df["CART_ID"].astype("Int64")
    df["USER_ID"]         = df["USER_ID"].astype("Int64")
    df["TOTAL"]           = df["TOTAL"].astype(float)
    df["DISCOUNTED_TOTAL"]= df["DISCOUNTED_TOTAL"].astype(float)
    df["TOTAL_PRODUCTS"]  = df["TOTAL_PRODUCTS"].astype("Int64")
    df["TOTAL_QUANTITY"]  = df["TOTAL_QUANTITY"].astype("Int64")
    df["EXTRACTED_AT"]    = pd.to_datetime(df["EXTRACTED_AT"], utc=True)

    log.info("Carts DataFrame → shape: %s", df.shape)
    log.info("Carts schema:\n%s", df.dtypes.to_string())
    return df


def build_cart_items_df(records: List[Dict]) -> pd.DataFrame:
    """
    Explode nested cart.products[] into flat line-item rows.
    This becomes fct_orders in dbt.
    
    Each row = one product inside one cart.
    """
    rows = []
    for cart in records:
        cart_id = cart.get("id")
        user_id = cart.get("userId")
        for item in cart.get("products", []):
            rows.append({
                "CART_ID"          : cart_id,
                "USER_ID"          : user_id,
                "PRODUCT_ID"       : item.get("id"),
                "PRODUCT_TITLE"    : item.get("title"),
                "PRICE"            : item.get("price"),
                "QUANTITY"         : item.get("quantity"),
                "LINE_TOTAL"       : item.get("total"),
                "DISCOUNT_PCT"     : item.get("discountPercentage"),
                "DISCOUNTED_TOTAL" : item.get("discountedTotal"),
                "EXTRACTED_AT"     : EXTRACTED_AT,
            })

    df = pd.DataFrame(rows)

    # ── Type casting
    df["CART_ID"]          = df["CART_ID"].astype("Int64")
    df["USER_ID"]          = df["USER_ID"].astype("Int64")
    df["PRODUCT_ID"]       = df["PRODUCT_ID"].astype("Int64")
    df["PRICE"]            = df["PRICE"].astype(float)
    df["QUANTITY"]         = df["QUANTITY"].astype("Int64")
    df["LINE_TOTAL"]       = df["LINE_TOTAL"].astype(float)
    df["DISCOUNT_PCT"]     = df["DISCOUNT_PCT"].astype(float)
    df["DISCOUNTED_TOTAL"] = df["DISCOUNTED_TOTAL"].astype(float)
    df["EXTRACTED_AT"]     = pd.to_datetime(df["EXTRACTED_AT"], utc=True)

    log.info("Cart Items DataFrame → shape: %s", df.shape)
    log.info("Cart Items schema:\n%s", df.dtypes.to_string())
    return df


# ─────────────────────────────────────────────
# 4.  SNOWFLAKE CONNECTION
# ─────────────────────────────────────────────
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Create Snowflake connection.
    Equivalent to bigquery.Client() in BQ.
    """
    log.info("Connecting to Snowflake account: %s", SF_CONFIG["account"])
    conn = snowflake.connector.connect(**SF_CONFIG)
    log.info("✅ Snowflake connected successfully")
    return conn


def setup_schemas(conn: snowflake.connector.SnowflakeConnection) -> None:
    """
    Auto-create schemas if they don't exist.
    Like ensuring BQ datasets exist before loading.
    """
    cursor = conn.cursor()
    schemas = ["RAW", "STAGING", "MARTS"]
    database = SF_CONFIG["database"]

    log.info("Setting up schemas in %s ...", database)
    cursor.execute(f"USE DATABASE {database}")

    for schema in schemas:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        log.info("  ✅ Schema ready → %s.%s", database, schema)

    cursor.close()


# ─────────────────────────────────────────────
# 5.  SNOWFLAKE UPLOAD  (auto schema detect)
# ─────────────────────────────────────────────
def upload_to_snowflake(
    conn       : snowflake.connector.SnowflakeConnection,
    df         : pd.DataFrame,
    schema     : str,
    table_name : str,
) -> None:
    """
    Upload DataFrame to Snowflake with AUTO schema detection.

    auto_create_table=True  → like BQ autodetect=True
                               reads pandas dtypes → creates Snowflake columns

    overwrite=True          → like BQ WRITE_TRUNCATE
                               drops + recreates table on each run

    Pandas dtype  →  Snowflake type
    ─────────────────────────────────
    Int64         →  NUMBER
    float64       →  FLOAT
    object        →  TEXT
    bool          →  BOOLEAN
    datetime64    →  TIMESTAMP_NTZ
    """
    database = SF_CONFIG["database"]
    log.info(
        "Uploading → %s.%s.%s (%d rows, %d cols)",
        database, schema, table_name, len(df), len(df.columns)
    )

    # Switch to correct schema
    conn.cursor().execute(f"USE SCHEMA {database}.{schema}")

    # ── write_pandas = BQ load_table_from_dataframe equivalent
    success, num_chunks, num_rows, output = write_pandas(
        conn              = conn,
        df                = df,
        table_name        = table_name,
        database          = database,
        schema            = schema,
        auto_create_table = True,   # ← AUTO SCHEMA DETECT (like BQ autodetect)
        overwrite         = True,   # ← WRITE_TRUNCATE (fresh load each run)
        quote_identifiers = False,
    )

    if success:
        log.info(
            "✅ Upload success → %s.%s | rows: %d | chunks: %d",
            schema, table_name, num_rows, num_chunks
        )
    else:
        log.error("❌ Upload failed → %s.%s", schema, table_name)
        raise RuntimeError(f"write_pandas failed for {schema}.{table_name}")


def print_schema_preview(df: pd.DataFrame, name: str) -> None:
    """Print detected schema before upload — like BQ schema preview."""
    log.info("─" * 50)
    log.info("Schema preview for: %s", name)
    log.info("─" * 50)

    TYPE_MAP = {
        "Int64"          : "NUMBER",
        "int64"          : "NUMBER",
        "float64"        : "FLOAT",
        "object"         : "TEXT",
        "bool"           : "BOOLEAN",
        "datetime64[ns, UTC]": "TIMESTAMP_NTZ",
    }

    for col, dtype in df.dtypes.items():
        sf_type = TYPE_MAP.get(str(dtype), "TEXT")
        log.info("  %-25s %-15s → %s", col, str(dtype), sf_type)
    log.info("─" * 50)


# ─────────────────────────────────────────────
# 6.  MAIN PIPELINE
# ─────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("🚀 E-Commerce Pipeline Starting")
    log.info("   Extracted at: %s", EXTRACTED_AT)
    log.info("=" * 55)

    # ── STEP 1: Parallel extraction (products + users + carts)
    log.info("\n📥 STEP 1: Parallel API Extraction")
    log.info("-" * 40)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_all_pages, "products", "/products"): "products",
            executor.submit(fetch_all_pages, "users",    "/users")   : "users",
            executor.submit(fetch_all_pages, "carts",    "/carts")   : "carts",
        }
        results = {}
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()

    # ── STEP 2: Build DataFrames + schema auto-detect
    log.info("\n🐼 STEP 2: Building DataFrames + Schema Detection")
    log.info("-" * 40)

    products_df   = build_products_df(results["products"])
    users_df      = build_users_df(results["users"])
    carts_df      = build_carts_df(results["carts"])
    cart_items_df = build_cart_items_df(results["carts"])  # explode from carts

    # Print schema previews (like BQ schema preview before load)
    print_schema_preview(products_df,   "RAW_PRODUCTS")
    print_schema_preview(users_df,      "RAW_USERS")
    print_schema_preview(carts_df,      "RAW_CARTS")
    print_schema_preview(cart_items_df, "RAW_CART_ITEMS")

    # ── STEP 3: Connect to Snowflake
    log.info("\n❄️  STEP 3: Snowflake Connection")
    log.info("-" * 40)
    conn = get_snowflake_connection()

    # ── STEP 4: Auto-create schemas
    log.info("\n🏗️  STEP 4: Setting up Schemas")
    log.info("-" * 40)
    setup_schemas(conn)

    # ── STEP 5: Upload all tables
    log.info("\n⬆️  STEP 5: Uploading to Snowflake")
    log.info("-" * 40)

    uploads = [
        (products_df,   "RAW", "RAW_PRODUCTS"),
        (users_df,      "RAW", "RAW_USERS"),
        (carts_df,      "RAW", "RAW_CARTS"),
        (cart_items_df, "RAW", "RAW_CART_ITEMS"),
    ]

    for df, schema, table in uploads:
        upload_to_snowflake(conn, df, schema, table)

    conn.close()
    log.info("Snowflake connection closed")

    # ── Summary
    log.info("\n" + "=" * 55)
    log.info("✅ Pipeline Complete!")
    log.info("=" * 55)
    log.info("  RAW.RAW_PRODUCTS   → %d rows", len(products_df))
    log.info("  RAW.RAW_USERS      → %d rows", len(users_df))
    log.info("  RAW.RAW_CARTS      → %d rows", len(carts_df))
    log.info("  RAW.RAW_CART_ITEMS → %d rows", len(cart_items_df))
    log.info("=" * 55)
    log.info("\nVerify in Snowflake:")
    log.info("  SELECT * FROM ECOMMERCE_DB.RAW.RAW_PRODUCTS   LIMIT 5;")
    log.info("  SELECT * FROM ECOMMERCE_DB.RAW.RAW_USERS      LIMIT 5;")
    log.info("  SELECT * FROM ECOMMERCE_DB.RAW.RAW_CARTS      LIMIT 5;")
    log.info("  SELECT * FROM ECOMMERCE_DB.RAW.RAW_CART_ITEMS LIMIT 5;")


if __name__ == "__main__":
    run()