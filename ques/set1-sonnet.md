# Practice Questions — Set 1

> Topics: SQL · Data Modeling · ETL & Pipeline Design · System Design · Distributed Systems & Integration

---

## Section 1 — SQL

---

### Q1: Top 3 Departments by Percentage of Employees Earning Over $100K (with ≥10 Employees)

#### Concept

Before writing any SQL, **define the grain**: one row per department in the output. This question layers multiple concepts:

- **Aggregation with conditional counting** (`COUNT(CASE WHEN ...)`) to compute a percentage
- **HAVING** to filter groups (not individual rows) — eliminates departments with fewer than 10 employees
- **Window functions** (`RANK` or `DENSE_RANK`) applied over the aggregated result to pick the top 3

A common mistake is filtering with WHERE on the percentage after aggregation, or misplacing HAVING. Another trap is dividing two integers in SQL — always cast to `FLOAT` to avoid integer division.

#### Sample Tables

**employees**

| employee_id | name       | department   | salary  |
|-------------|------------|--------------|---------|
| 1           | Alice       | Engineering  | 120,000 |
| 2           | Bob         | Engineering  | 95,000  |
| 3           | Carol       | Marketing    | 105,000 |
| 4           | Dave        | Marketing    | 80,000  |
| 5           | Eve         | Engineering  | 115,000 |
| ...         | ...         | ...          | ...     |

#### SQL Solution

```sql
WITH dept_stats AS (
    SELECT
        department,
        COUNT(*)                                                    AS total_employees,
        COUNT(CASE WHEN salary > 100000 THEN 1 END)                AS high_earners,
        ROUND(
            COUNT(CASE WHEN salary > 100000 THEN 1 END) * 100.0
            / COUNT(*),
            2
        )                                                           AS pct_high_earners
    FROM employees
    GROUP BY department
    HAVING COUNT(*) >= 10          -- exclude departments with fewer than 10 employees
),
ranked AS (
    SELECT
        department,
        total_employees,
        high_earners,
        pct_high_earners,
        RANK() OVER (ORDER BY pct_high_earners DESC)               AS rnk
    FROM dept_stats
)
SELECT department, total_employees, high_earners, pct_high_earners
FROM ranked
WHERE rnk <= 3
ORDER BY rnk;
```

#### Why `RANK` over `ROW_NUMBER`?

| Function       | Ties handled how?                        | Use when...                             |
|----------------|------------------------------------------|-----------------------------------------|
| `ROW_NUMBER`   | Arbitrary tiebreak — one row gets rank 1 | You need exactly N rows                 |
| `RANK`         | Ties share a rank; gaps after ties       | Honest ranking, may return >3 rows      |
| `DENSE_RANK`   | Ties share a rank; no gaps               | Leaderboard-style, no missing ranks     |

> **Tip:** State the grain before writing. In production reporting pipelines at Apple, aggregation bugs on misidentified grains are a leading cause of incorrect dashboards.

---

### Q2: First-Touch Attribution Channel for Each Converting User

#### Concept

**Attribution** answers: "which marketing channel gets credit for a conversion?" First-touch gives 100% credit to the *first* channel a user ever interacted with before converting.

Key SQL skills tested:

- `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_ts ASC)` — rank events per user chronologically
- Filtering for the row numbered 1 gives the first touchpoint
- Must handle: **late-arriving events**, **duplicate events** (same user + same channel + same timestamp), and users who converted without any prior touch (NULL channel)

#### Sample Tables

**events**

| event_id | user_id | channel   | event_type  | event_ts            |
|----------|---------|-----------|-------------|---------------------|
| 1        | u001    | organic   | page_view   | 2024-01-01 08:00:00 |
| 2        | u001    | paid_search | click     | 2024-01-01 09:00:00 |
| 3        | u001    | email     | conversion  | 2024-01-02 10:00:00 |
| 4        | u002    | social    | page_view   | 2024-01-03 11:00:00 |
| 5        | u002    | social    | conversion  | 2024-01-03 12:00:00 |
| 6        | u003    | NULL      | conversion  | 2024-01-04 09:00:00 |

#### SQL Solution

```sql
WITH pre_conversion_touches AS (
    -- Only consider touchpoints that occurred BEFORE the conversion
    SELECT
        e.user_id,
        e.channel,
        e.event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY e.user_id
            ORDER BY e.event_ts ASC
        )                                               AS touch_rank
    FROM events e
    -- Join to the conversion timestamp for each user
    JOIN (
        SELECT user_id, MIN(event_ts) AS converted_at
        FROM events
        WHERE event_type = 'conversion'
        GROUP BY user_id
    ) conv ON e.user_id = conv.user_id
         AND e.event_ts <= conv.converted_at
         AND e.event_type != 'conversion'   -- exclude the conversion event itself
),
first_touch AS (
    SELECT user_id, channel AS first_touch_channel
    FROM pre_conversion_touches
    WHERE touch_rank = 1
)
SELECT
    conv.user_id,
    COALESCE(ft.first_touch_channel, 'direct / unknown') AS first_touch_channel,
    conv.converted_at
FROM (
    SELECT user_id, MIN(event_ts) AS converted_at
    FROM events
    WHERE event_type = 'conversion'
    GROUP BY user_id
) conv
LEFT JOIN first_touch ft ON conv.user_id = ft.user_id;
```

#### Edge Cases to Call Out in an Interview

| Scenario                          | Handling                                                       |
|-----------------------------------|----------------------------------------------------------------|
| Duplicate events (same ts + channel) | Add `event_id` as tiebreaker in `ORDER BY`                |
| Late-arriving events              | Re-run the CTE after ingestion; or use a reprocessing window  |
| User converted with no prior touch | `LEFT JOIN` + `COALESCE` returns "direct / unknown"          |
| Multiple conversions per user     | `MIN(event_ts)` isolates the first conversion only            |

> **Tip:** In a streaming pipeline, late events require a reprocessing strategy — e.g., watermarks in Flink/Spark Structured Streaming or a correction batch job.

---

## Section 2 — Data Modeling

---

### Q3: Star Schema vs Snowflake Schema — Differences and When to Use Each

#### Concept

Both schemas are **multidimensional models** designed for OLAP (Online Analytical Processing) workloads. They differ in how dimension tables are structured.

**Star Schema** — dimensions are flat (denormalized). The fact table sits in the center and joins directly to each dimension. Shape resembles a star.

**Snowflake Schema** — dimensions are normalized into sub-dimension tables (hierarchies are broken out). Shape resembles a snowflake.

```
-- Star: fact_sales joins directly to dim_product
fact_sales --> dim_product (product_id, product_name, category, brand)

-- Snowflake: category and brand live in their own tables
fact_sales --> dim_product (product_id, product_name, category_id, brand_id)
                    --> dim_category (category_id, category_name)
                    --> dim_brand   (brand_id, brand_name)
```

#### Comparison Table

| Dimension            | Star Schema                            | Snowflake Schema                        |
|----------------------|----------------------------------------|-----------------------------------------|
| Normalization        | Denormalized (1NF / 2NF)               | Normalized (3NF)                        |
| Number of joins      | Few (1 join per dimension)             | Many (multiple joins per hierarchy)     |
| Query performance    | Faster — fewer joins                   | Slower — more joins                     |
| Storage usage        | Higher — repeated attribute values     | Lower — attributes stored once          |
| ETL complexity       | Simpler — fewer tables to maintain     | More complex — maintain hierarchy tables|
| BI tool compatibility| Excellent — most tools prefer star     | Good but requires more metadata config  |
| Update flexibility   | Harder — update repeated values        | Easier — update one hierarchy row       |
| Typical use case     | BI dashboards, ad hoc analysis         | Large catalogs with deep hierarchies    |

#### DDL Example

```sql
-- === STAR SCHEMA ===
CREATE TABLE dim_product_star (
    product_sk    INT PRIMARY KEY,  -- surrogate key
    product_id    VARCHAR(20),      -- natural key
    product_name  VARCHAR(100),
    category      VARCHAR(50),      -- denormalized: category lives here
    brand         VARCHAR(50)       -- denormalized: brand lives here
);

-- === SNOWFLAKE SCHEMA ===
CREATE TABLE dim_category (
    category_sk   INT PRIMARY KEY,
    category_name VARCHAR(50)
);

CREATE TABLE dim_brand (
    brand_sk      INT PRIMARY KEY,
    brand_name    VARCHAR(50)
);

CREATE TABLE dim_product_snowflake (
    product_sk    INT PRIMARY KEY,
    product_id    VARCHAR(20),
    product_name  VARCHAR(100),
    category_sk   INT REFERENCES dim_category(category_sk),
    brand_sk      INT REFERENCES dim_brand(brand_sk)
);
```

> **Rule of thumb:** Start with a star schema. Normalize to snowflake only when storage costs are material or when a hierarchy (like a product taxonomy with 5+ levels) changes frequently enough that updating denormalized values becomes painful.

---

### Q4: Design a Data Warehouse for an Online Retail Store (Star Schema)

#### Concept — Grain-First Thinking

The Kimball approach demands you answer **three questions** before drawing any table:

1. **What business process** are you modeling? (e.g., order line items)
2. **What is the grain?** (e.g., one row per order line item)
3. **What facts and dimensions** are relevant at that grain?

For a retail store, the primary fact process is **order line items**. Each row in the fact table represents one product purchased in one order.

#### Schema Overview

```
                    dim_date
                       |
dim_customer ---  fact_order_lines  --- dim_product
                       |
                    dim_store
```

#### Table Definitions with Sample Data

**dim_customer**

| customer_sk | customer_id | first_name | last_name | email                | city       | country | effective_from | effective_to | is_current |
|-------------|-------------|------------|-----------|----------------------|------------|---------|----------------|--------------|------------|
| 1001        | C-001       | Alice       | Kim       | alice@example.com    | New York   | US      | 2022-01-01     | 9999-12-31   | Y          |
| 1002        | C-002       | Bob         | Lee       | bob@example.com      | London     | GB      | 2021-06-15     | 9999-12-31   | Y          |

**dim_product**

| product_sk | product_id | product_name | category    | brand   | unit_price |
|------------|------------|--------------|-------------|---------|------------|
| 2001       | P-1001     | iPhone 15    | Electronics | Apple   | 999.00     |
| 2002       | P-1002     | AirPods Pro  | Accessories | Apple   | 249.00     |

**dim_date**

| date_sk  | full_date  | year | quarter | month | month_name | week | day_of_week | is_holiday |
|----------|------------|------|---------|-------|------------|------|-------------|------------|
| 20240101 | 2024-01-01 | 2024 | Q1      | 1     | January    | 1    | Monday      | Y          |
| 20240115 | 2024-01-15 | 2024 | Q1      | 1     | January    | 3    | Monday      | N          |

**fact_order_lines** (grain: one row per order line item)

| order_line_sk | order_id | customer_sk | product_sk | date_sk  | store_sk | quantity | unit_price | discount_amt | gross_revenue | net_revenue |
|---------------|----------|-------------|------------|----------|----------|----------|------------|--------------|---------------|-------------|
| 1             | ORD-001  | 1001        | 2001       | 20240115 | 3001     | 1        | 999.00     | 50.00        | 999.00        | 949.00      |
| 2             | ORD-001  | 1001        | 2002       | 20240115 | 3001     | 2        | 249.00     | 0.00         | 498.00        | 498.00      |

```sql
CREATE TABLE fact_order_lines (
    order_line_sk   BIGINT PRIMARY KEY,
    order_id        VARCHAR(20),
    customer_sk     INT REFERENCES dim_customer(customer_sk),
    product_sk      INT REFERENCES dim_product(product_sk),
    date_sk         INT REFERENCES dim_date(date_sk),
    store_sk        INT REFERENCES dim_store(store_sk),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount_amt    DECIMAL(10,2),
    gross_revenue   DECIMAL(10,2),
    net_revenue     DECIMAL(10,2)
);
```

> **Tip:** Store pre-calculated measures like `gross_revenue` in the fact table to avoid repeated computation at query time. Keep only numeric, additive facts — do not store text in the fact table.

---

### Q5: Surrogate Keys and SCD Type 2 (Slowly Changing Dimensions)

#### Concept — What Are Surrogate Keys?

A **surrogate key** is an artificially generated integer (or hash) that serves as the primary key of a dimension row. It is **not** derived from the source system — that identifier is called the **natural key** (or business key).

Why surrogate keys matter:

- Source systems change natural keys (customer number formats change, mergers happen)
- Multiple source systems may use conflicting key formats
- SCD Type 2 requires **multiple rows** for the same entity — each row needs its own PK

#### SCD Types at a Glance

| SCD Type | Strategy                              | History Preserved? | Example Use Case              |
|----------|---------------------------------------|--------------------|-------------------------------|
| Type 0   | Never overwrite                       | N/A (static)       | Date of birth                 |
| Type 1   | Overwrite old value                   | No                 | Fixing a typo in a name       |
| Type 2   | Insert new row with new surrogate key | Yes (full history) | Customer address change       |
| Type 3   | Add a "previous value" column         | Partial (1 prior)  | Tracking prior marketing tier |
| Type 6   | Hybrid of 1+2+3                       | Yes + current flag | Complex analyst needs         |

#### SCD Type 2 in Detail

When a tracked attribute changes (e.g., a customer moves city), you:

1. **Expire** the current row by setting `effective_to` and `is_current = 'N'`
2. **Insert** a new row with the updated attribute, a new surrogate key, and `is_current = 'Y'`

The natural key (`customer_id`) is the "glue" that links all historical rows for the same customer.

**Required administrative columns for Type 2:**

| Column            | Purpose                                               |
|-------------------|-------------------------------------------------------|
| `surrogate_sk`    | Unique PK for this specific version of the record     |
| `natural_key`     | Business key linking all versions (e.g., customer_id) |
| `effective_from`  | When this version became active                       |
| `effective_to`    | When this version was superseded (9999-12-31 = current)|
| `is_current`      | Flag for fast "current record" filter                 |
| `change_reason`   | Optional but recommended — why did this change?       |

#### Sample: Customer Changes City

**Before change** (Alice moves from New York to San Francisco):

| customer_sk | customer_id | name  | city         | effective_from | effective_to | is_current |
|-------------|-------------|-------|--------------|----------------|--------------|------------|
| 1001        | C-001       | Alice | New York     | 2022-01-01     | 9999-12-31   | Y          |

**After ETL processes the change:**

| customer_sk | customer_id | name  | city          | effective_from | effective_to | is_current |
|-------------|-------------|-------|---------------|----------------|--------------|------------|
| 1001        | C-001       | Alice | New York      | 2022-01-01     | 2024-03-15   | N          |
| 1057        | C-001       | Alice | San Francisco | 2024-03-16     | 9999-12-31   | Y          |

#### ETL Logic (Python-style pseudocode)

```python
def process_scd2_change(conn, customer_id, new_city, change_date):
    # Step 1: Expire the current row
    conn.execute("""
        UPDATE dim_customer
        SET effective_to = :change_date - INTERVAL '1 day',
            is_current   = 'N'
        WHERE customer_id = :cid
          AND is_current  = 'Y'
    """, {"cid": customer_id, "change_date": change_date})

    # Step 2: Insert the new version with a fresh surrogate key
    conn.execute("""
        INSERT INTO dim_customer
            (customer_sk, customer_id, city, effective_from, effective_to, is_current)
        VALUES
            (nextval('dim_customer_sk_seq'), :cid, :city, :eff_from, '9999-12-31', 'Y')
    """, {"cid": customer_id, "city": new_city, "eff_from": change_date})
```

#### Querying Current vs Historical

```sql
-- Get only current customer records
SELECT * FROM dim_customer WHERE is_current = 'Y';

-- Reconstruct what a customer's city was at order time
-- (the fact table stores the surrogate key from the time of the event)
SELECT
    f.order_id,
    c.name,
    c.city          AS city_at_time_of_order,
    f.net_revenue
FROM fact_order_lines f
JOIN dim_customer c ON f.customer_sk = c.customer_sk;   -- exact version, no date filtering needed
```

> **Key insight:** Because the fact table stores the surrogate key frozen at the time of the transaction, historical analysis is automatic — no date range filtering needed on the dimension.

---

### Q6: Data Vault — Hubs, Links, and Satellites

#### Concept

Data Vault is a **raw data warehouse modeling methodology** designed for auditability, scalability, and resilience to changing business rules. It separates concerns into three distinct object types:

| Object     | What it stores                                     | Analogy                     |
|------------|----------------------------------------------------|-----------------------------|
| **Hub**    | Business keys — the unique identifiers from source  | A noun (Customer, Order)    |
| **Link**   | Relationships between hubs (associations)           | A verb (Customer PLACED Order)|
| **Satellite** | Descriptive attributes and their history         | An adjective (Customer details)|

**Core principles:**

- Hubs and Links **never change** — they are insert-only
- Satellites **track history** via load timestamps (like SCD Type 2 but automatic)
- Everything has a `load_dts` (load date/time) and `rec_src` (record source) for full auditability
- Hash keys replace surrogate sequences — derived deterministically from the business key, enabling parallel loading

#### Schema Definitions

**hub_customer** — stores every unique customer business key

```sql
CREATE TABLE hub_customer (
    hub_customer_hk   CHAR(32)     PRIMARY KEY,   -- MD5/SHA hash of customer_id
    customer_id       VARCHAR(20)  NOT NULL,       -- natural/business key from source
    load_dts          TIMESTAMP    NOT NULL,
    rec_src           VARCHAR(50)  NOT NULL        -- e.g., 'CRM', 'ECOMMERCE_WEB'
);
```

| hub_customer_hk (hash)          | customer_id | load_dts            | rec_src       |
|---------------------------------|-------------|---------------------|---------------|
| a3f2...                         | C-001       | 2024-01-01 08:00:00 | ECOMMERCE_WEB |
| b91c...                         | C-002       | 2024-01-02 09:00:00 | CRM           |

**hub_order**

```sql
CREATE TABLE hub_order (
    hub_order_hk   CHAR(32)     PRIMARY KEY,
    order_id       VARCHAR(20)  NOT NULL,
    load_dts       TIMESTAMP    NOT NULL,
    rec_src        VARCHAR(50)  NOT NULL
);
```

**lnk_order_customer** — relationship: a customer placed an order

```sql
CREATE TABLE lnk_order_customer (
    lnk_order_customer_hk   CHAR(32)  PRIMARY KEY,   -- hash of (order_id + customer_id)
    hub_order_hk            CHAR(32)  REFERENCES hub_order(hub_order_hk),
    hub_customer_hk         CHAR(32)  REFERENCES hub_customer(hub_customer_hk),
    load_dts                TIMESTAMP NOT NULL,
    rec_src                 VARCHAR(50) NOT NULL
);
```

| lnk_order_customer_hk | hub_order_hk | hub_customer_hk | load_dts            | rec_src       |
|-----------------------|--------------|-----------------|---------------------|---------------|
| d44f...               | e7a1...      | a3f2...         | 2024-01-15 10:05:00 | ECOMMERCE_WEB |

**sat_customer** — descriptive attributes with full history

```sql
CREATE TABLE sat_customer (
    hub_customer_hk   CHAR(32)     NOT NULL REFERENCES hub_customer(hub_customer_hk),
    load_dts          TIMESTAMP    NOT NULL,
    load_end_dts      TIMESTAMP,                     -- NULL = currently active record
    hash_diff         CHAR(32)     NOT NULL,          -- hash of all payload columns for change detection
    customer_name     VARCHAR(100),
    email             VARCHAR(150),
    city              VARCHAR(80),
    country           CHAR(2),
    rec_src           VARCHAR(50)  NOT NULL,
    PRIMARY KEY (hub_customer_hk, load_dts)
);
```

| hub_customer_hk | load_dts            | load_end_dts        | customer_name | city          | country |
|-----------------|---------------------|---------------------|---------------|---------------|---------|
| a3f2...         | 2022-01-01 00:00:00 | 2024-03-15 23:59:59 | Alice Kim     | New York      | US      |
| a3f2...         | 2024-03-16 00:00:00 | NULL                | Alice Kim     | San Francisco | US      |

#### Business Use-Case Walkthrough (E-commerce)

```
Sources:          CRM System                Web Events / Orders
                     |                             |
                 hub_customer  <--- lnk_order_customer ---> hub_order
                     |                                          |
                 sat_customer                             sat_order_details
                 (name, email,                            (total_amt, status,
                  city, tier)                              shipped_ts)
```

1. A new customer registers → row inserted into `hub_customer` and `sat_customer`
2. Customer places an order → row in `hub_order`, row in `lnk_order_customer`
3. Customer updates their email → **new row** in `sat_customer` (old row gets `load_end_dts` set); hub untouched
4. Order ships → new row in `sat_order_details` with updated status

#### Data Vault vs Kimball Star Schema

| Dimension           | Data Vault                                  | Kimball Star                         |
|---------------------|---------------------------------------------|--------------------------------------|
| Primary goal        | Auditability, full raw history              | Fast analytical queries              |
| ETL pattern         | Insert-only, parallel loads                 | Upserts / SCD logic                  |
| Schema changes      | Resilient — add a satellite, done           | May require ETL rewrites             |
| Query complexity    | High — business vault / mart layer on top   | Low — direct star joins              |
| Best for            | Regulated industries, complex integrations  | BI / self-service analytics          |

> **Tip:** Data Vault is typically used as the **raw vault** (landing/staging layer). Business-facing reporting is served from a **Business Vault** or an **Information Mart** built on top — often a star schema derived from the vault.

---

## Section 3 — ETL & Pipeline Design

---

### Q7: ETL Pipeline for Stripe Payment Events → Analytics Warehouse

#### Concept

A production-grade payment pipeline must be:

- **Idempotent** — rerunning the same job produces the same result (no double-counting)
- **Deduplication-aware** — Stripe may deliver the same webhook event more than once
- **Schema-evolution-safe** — Stripe adds new fields; your pipeline should not break
- **Auditable** — raw events must be retained separately from transformed data

#### Pipeline Architecture

| Layer          | Purpose                                         | Typical Tool / Storage                  |
|----------------|-------------------------------------------------|-----------------------------------------|
| **Ingestion**  | Receive Stripe webhook events, write to raw store | Kafka / S3 raw bucket / GCS            |
| **Raw Layer**  | Immutable, append-only event store              | S3 (Parquet/JSON) / BigQuery raw dataset|
| **Staging**    | Parse, cast types, deduplicate                  | dbt staging models / Spark job          |
| **Core/ODS**   | Joined, enriched, normalized events             | Data warehouse (Snowflake, Redshift)    |
| **Mart**       | Business-level aggregations (revenue, MRR)      | Materialized views / summary tables     |
| **Monitoring** | Row count checks, null audits, SLA alerts       | Great Expectations / Monte Carlo        |

#### Key Pipeline Decisions

**1. Ingestion method — Webhooks vs API polling**

| Method          | Pros                               | Cons                                    |
|-----------------|------------------------------------|-----------------------------------------|
| Webhooks (push) | Real-time, lower API quota usage   | Must handle retries / duplicates        |
| API polling     | Simple, full control of schedule   | Latency, rate limits, harder to backfill|

**2. Deduplication**

Stripe guarantees *at-least-once* delivery. Deduplicate on `event_id` (Stripe's unique event identifier):

```python
def deduplicate_events(df):
    """
    Stripe sends duplicate events on retries.
    Keep only the first occurrence of each event_id.
    """
    return (
        df
        .sort_values("created_at")               # earliest first
        .drop_duplicates(subset=["event_id"], keep="first")
    )
```

**3. Idempotent load pattern (Python + SQLAlchemy)**

```python
def load_payments_idempotent(conn, batch_df, batch_date: str):
    """
    Use INSERT ... ON CONFLICT DO NOTHING to make every run safe to re-execute.
    """
    rows = batch_df.to_dict(orient="records")

    insert_sql = """
        INSERT INTO stg_stripe_events
            (event_id, event_type, customer_id, amount_cents, currency, created_at)
        VALUES
            (:event_id, :event_type, :customer_id, :amount_cents, :currency, :created_at)
        ON CONFLICT (event_id) DO NOTHING   -- idempotency guard
    """
    conn.execute(insert_sql, rows)
```

**4. Schema evolution strategy**

```python
import json

def parse_stripe_event(raw: str) -> dict:
    """
    Always parse from raw JSON.
    Use .get() with defaults so new fields don't crash older code paths.
    """
    event = json.loads(raw)
    data  = event.get("data", {}).get("object", {})
    return {
        "event_id"     : event.get("id"),
        "event_type"   : event.get("type"),
        "customer_id"  : data.get("customer"),
        "amount_cents" : data.get("amount", 0),
        "currency"     : data.get("currency", "usd"),
        "created_at"   : event.get("created"),
        "raw_payload"  : raw,           # always store the full raw event
    }
```

> **Tip:** Always retain the raw JSON payload. When Stripe adds new fields or you need to reprocess history with new business logic, you can replay from the raw layer without re-calling the API.

---

### Q8: Process a 100 GB CSV Without Loading It Entirely Into Memory

#### Concept

The core idea is **chunked (streaming) reads** — process the file in fixed-size batches that fit in available memory. Each chunk is transformed and written to the output independently.

Three additional concerns for production:

- **Checkpointing** — track which byte offset or chunk index you finished, so a crash resumes rather than restarts
- **Parallelism** — split the file across workers (careful with CSV headers)
- **Output atomicity** — write chunks to a temp location, then rename/commit to avoid corrupt partial outputs

#### Strategy Overview

| Strategy             | Description                                          | Best For                               |
|----------------------|------------------------------------------------------|----------------------------------------|
| Chunked Pandas reads | `pd.read_csv(chunksize=N)` iterates rows in batches  | Single-node, simple transformations    |
| Dask / Polars        | Parallel chunked processing with lazy evaluation     | Multi-core, complex transforms         |
| Spark                | Distributed across a cluster                         | Truly massive files, cluster available |
| Shell split + parallel | `split` file then process in parallel processes   | Quick ad-hoc jobs                      |

#### Python — Chunked Read with Checkpointing

```python
import pandas as pd
import os
import json

CHUNK_SIZE  = 500_000           # rows per batch (~500 MB if ~1KB/row)
INPUT_FILE  = "data/large.csv"
OUTPUT_DIR  = "output/"
CHECKPOINT  = "checkpoint.json"

def load_checkpoint() -> int:
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            return json.load(f)["last_chunk"]
    return -1

def save_checkpoint(chunk_idx: int):
    with open(CHECKPOINT, "w") as f:
        json.dump({"last_chunk": chunk_idx}, f)

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["user_id", "event_ts"])     # require key fields
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

def process_file():
    last_done = load_checkpoint()

    reader = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE)
    for chunk_idx, chunk in enumerate(reader):
        if chunk_idx <= last_done:
            print(f"Skipping chunk {chunk_idx} (already processed)")
            continue

        cleaned = clean_chunk(chunk)

        # Write to a temp file first, then rename (atomic swap)
        tmp_path  = f"{OUTPUT_DIR}/chunk_{chunk_idx}.tmp.parquet"
        done_path = f"{OUTPUT_DIR}/chunk_{chunk_idx}.parquet"
        cleaned.to_parquet(tmp_path, index=False)
        os.rename(tmp_path, done_path)

        save_checkpoint(chunk_idx)
        print(f"Processed chunk {chunk_idx}: {len(cleaned):,} rows")

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    process_file()
```

> **Tip:** The atomic `os.rename` (temp → final) prevents corrupt partial output files from polluting your output directory if the process crashes mid-write.

---

### Q9: Sort a 100 GB File With Only 10 GB of RAM (External Merge Sort)

#### Concept

You cannot sort the entire file in memory. **External merge sort** solves this in two phases:

1. **Sort phase** — read N-GB chunks, sort each chunk in memory, write sorted chunks (called *runs*) to disk
2. **Merge phase** — perform a **k-way merge** across all sorted runs using a min-heap, streaming the globally sorted output

For 100 GB with 10 GB RAM:

- Phase 1 produces ~10 sorted runs of ~10 GB each
- Phase 2 allocates ~9 GB as input buffers (one per run, ~1 GB each) + ~1 GB output buffer
- The heap always contains at most k=10 elements at a time

#### Phase Breakdown

| Phase       | Steps                                             | I/O pattern          |
|-------------|---------------------------------------------------|----------------------|
| Sort phase  | Read 10 GB → quicksort in RAM → write run to disk | Sequential read + write |
| Merge phase | Open 10 runs, k-way merge via min-heap            | Sequential multi-read |
| Output      | Stream merged result to final file                | Sequential write      |

```
Disk:  [100 GB unsorted file]
           |
    Split into 10 chunks (10 GB each)
           |
    Sort each chunk in memory → write sorted run
           |
    [run_0.sorted] [run_1.sorted] ... [run_9.sorted]
           |
    k-way merge (min-heap of 10 elements, one from each run)
           |
    [100 GB sorted file]
```

#### Python Implementation Sketch

```python
import heapq
import os

CHUNK_SIZE_BYTES = 9 * 1024 ** 3   # 9 GB (leave 1 GB headroom)
INPUT_FILE       = "huge.txt"
RUN_DIR          = "runs/"

# ─── Phase 1: Create sorted runs ─────────────────────────────────────────────

def create_sorted_runs(input_file: str, run_dir: str) -> list[str]:
    os.makedirs(run_dir, exist_ok=True)
    run_paths = []
    buffer    = []
    buf_bytes = 0
    run_idx   = 0

    with open(input_file, "r") as f:
        for line in f:
            buffer.append(line.rstrip("\n"))
            buf_bytes += len(line)

            if buf_bytes >= CHUNK_SIZE_BYTES:
                buffer.sort()
                run_path = f"{run_dir}/run_{run_idx}.txt"
                with open(run_path, "w") as out:
                    out.write("\n".join(buffer) + "\n")
                run_paths.append(run_path)
                buffer, buf_bytes, run_idx = [], 0, run_idx + 1

    if buffer:                       # flush remaining lines
        buffer.sort()
        run_path = f"{run_dir}/run_{run_idx}.txt"
        with open(run_path, "w") as out:
            out.write("\n".join(buffer) + "\n")
        run_paths.append(run_path)

    return run_paths

# ─── Phase 2: k-way merge using heapq ────────────────────────────────────────

def merge_sorted_runs(run_paths: list[str], output_file: str):
    """
    heapq.merge does a k-way merge using a min-heap internally.
    Each file iterator streams one line at a time — O(k) memory.
    """
    file_handles = [open(p, "r") for p in run_paths]
    iterators    = (line.rstrip("\n") for fh in file_handles for line in fh)

    with open(output_file, "w") as out:
        for sorted_line in heapq.merge(*[open(p) for p in run_paths]):
            out.write(sorted_line)

    for fh in file_handles:
        fh.close()

# ─── Main ─────────────────────────────────────────────────────────────────────

runs   = create_sorted_runs(INPUT_FILE, RUN_DIR)
merge_sorted_runs(runs, "sorted_output.txt")
```

#### Complexity

| Metric     | Value                                         |
|------------|-----------------------------------------------|
| Time       | O(N log N) — dominated by sort + merge passes |
| Space      | O(k) heap + I/O buffers (~10 GB RAM max)       |
| Disk reads | ~2× file size (one pass each phase)            |
| Scalability| Add merge passes for even larger files (tree of merges) |

> **Tip:** In a real system, use buffered I/O and tune buffer sizes based on the OS page size for optimal disk throughput. Spark's `sortWithinPartitions` and external sort algorithms in databases (e.g., PostgreSQL's `work_mem`) follow this same pattern internally.

---

## Section 4 — System Design

---

### Q10: Store and Query Hundreds of Millions of Daily Clickstream Events with 2-Year Retention

#### Concept

At hundreds of millions of events per day × 730 days = **70–150+ billion rows** over 2 years. This requires:

- **Storage tiering** — not all data is queried equally; recent data is hot, old data is cold
- **Columnar formats** — Parquet/ORC compress 10–20× better than CSV and enable predicate pushdown
- **Partition pruning** — partition by date (and optionally event type or region) so queries scan only relevant files
- **Cost vs latency tradeoff** — sub-second queries on recent data; accepting minutes on 2-year-old data is acceptable and far cheaper

#### Storage Tier Architecture

| Tier    | Data Age      | Format           | Query Engine            | Storage Layer           | Relative Cost | Typical Query Latency |
|---------|---------------|------------------|-------------------------|-------------------------|---------------|-----------------------|
| Hot     | 0 – 30 days   | Parquet (columnar)| BigQuery / Snowflake    | SSD-backed cloud DW     | High          | < 10 seconds          |
| Warm    | 31 – 180 days | Parquet + Zstd   | Athena / Presto on S3   | S3 Standard             | Medium        | 10 – 60 seconds       |
| Cold    | 181 days – 2y | Parquet + Snappy | Athena / Spark batch    | S3 Glacier Instant      | Low           | 1 – 5 minutes         |

#### Partition Strategy

```
s3://clickstream-data/
  events/
    year=2024/
      month=01/
        day=15/
          event_type=page_view/
            part-0001.parquet
            part-0002.parquet
```

```sql
-- Query only scans Jan 2024 files — eliminates 364/365 days of data
SELECT
    event_type,
    COUNT(*)        AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM clickstream.events
WHERE year = '2024'
  AND month = '01'
  AND day   BETWEEN '01' AND '31'
GROUP BY event_type;
```

#### Schema Design for Clickstream

```sql
CREATE TABLE clickstream_events (
    event_id        VARCHAR(36),       -- UUID
    user_id         VARCHAR(50),
    session_id      VARCHAR(50),
    event_type      VARCHAR(50),       -- page_view, click, add_to_cart, purchase
    page_url        VARCHAR(500),
    referrer        VARCHAR(500),
    device_type     VARCHAR(20),       -- mobile, desktop, tablet
    country_code    CHAR(2),
    event_ts        TIMESTAMP,
    properties      VARCHAR(MAX)       -- JSON blob for flexible event-specific attrs
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='ZSTD');
```

#### Lifecycle Automation (AWS example)

```json
{
  "Rules": [
    {
      "ID": "ClickstreamTiering",
      "Filter": {"Prefix": "events/"},
      "Transitions": [
        {"Days": 30,  "StorageClass": "STANDARD_IA"},
        {"Days": 180, "StorageClass": "GLACIER_IR"}
      ],
      "Expiration": {"Days": 730}
    }
  ]
}
```

#### Cost Estimation (rough)

| Layer      | Daily Events | Daily Storage    | Monthly Storage Cost (est.) |
|------------|--------------|------------------|-----------------------------|
| Hot (DW)   | 200M events  | ~50 GB/day       | ~$300 (compute + storage)   |
| Warm (S3)  | 200M events  | ~20 GB/day Parquet| ~$30 (storage only)         |
| Cold (Glacier) | 200M events | ~15 GB/day    | ~$5 (Glacier IR pricing)    |

> **Tip:** Use `ZSTD` compression for Parquet on Spark/Athena workloads — it typically achieves 3–5× better compression than `Snappy` at modest CPU cost, meaningfully reducing both storage costs and scan bytes.

---

### Q11: Optimize OLAP Aggregations for Monthly and Quarterly Performance Reports

#### Concept

When analysts or dashboards run `GROUP BY month` / `GROUP BY quarter` queries over billions of rows, full table scans become slow and expensive. The solution is **pre-aggregation** — compute summaries on a schedule and store them in smaller tables that queries can hit directly.

**Three main strategies:**

| Strategy               | Description                                                   | Latency        | Freshness           |
|------------------------|---------------------------------------------------------------|----------------|---------------------|
| Materialized views     | DB-native; query-transparent; auto or manual refresh          | Near real-time | Minutes to hours    |
| Summary/aggregate tables | Explicit pre-aggregated table populated by scheduled job   | Batch window   | Daily / hourly      |
| Incremental roll-ups   | Only re-aggregate changed partitions (e.g., yesterday's data) | Fast batches   | Near-daily          |

#### Pre-aggregated Summary Table

```sql
-- Base aggregation table — populated nightly
CREATE TABLE agg_revenue_monthly (
    report_year     INT,
    report_month    INT,
    product_category VARCHAR(50),
    region           CHAR(2),
    total_orders     BIGINT,
    total_revenue    DECIMAL(18,2),
    avg_order_value  DECIMAL(10,2),
    unique_customers BIGINT,
    refreshed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_year, report_month, product_category, region)
);

-- Nightly refresh: incremental — only recompute last 2 months (handles late data)
INSERT INTO agg_revenue_monthly
    (report_year, report_month, product_category, region,
     total_orders, total_revenue, avg_order_value, unique_customers)
SELECT
    EXTRACT(YEAR  FROM o.order_date)::INT    AS report_year,
    EXTRACT(MONTH FROM o.order_date)::INT    AS report_month,
    p.category                               AS product_category,
    c.country                                AS region,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    SUM(f.net_revenue)                       AS total_revenue,
    AVG(f.net_revenue)                       AS avg_order_value,
    COUNT(DISTINCT o.customer_sk)            AS unique_customers
FROM fact_order_lines f
JOIN dim_product  p ON f.product_sk  = p.product_sk
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_date     d ON f.date_sk     = d.date_sk
JOIN (SELECT DISTINCT order_id, order_date, customer_sk FROM fact_order_lines) o
     ON f.order_id = o.order_id
WHERE d.full_date >= CURRENT_DATE - INTERVAL '60 days'   -- incremental window
GROUP BY 1, 2, 3, 4
ON CONFLICT (report_year, report_month, product_category, region)
DO UPDATE SET
    total_orders     = EXCLUDED.total_orders,
    total_revenue    = EXCLUDED.total_revenue,
    avg_order_value  = EXCLUDED.avg_order_value,
    unique_customers = EXCLUDED.unique_customers,
    refreshed_at     = CURRENT_TIMESTAMP;
```

#### Query Against Summary Table (instant)

```sql
-- Quarterly revenue by category — reads from tiny agg table, not billions of rows
SELECT
    report_year,
    CEIL(report_month / 3.0)::INT     AS quarter,
    product_category,
    SUM(total_revenue)                AS quarterly_revenue,
    SUM(total_orders)                 AS quarterly_orders
FROM agg_revenue_monthly
WHERE report_year = 2024
GROUP BY report_year, quarter, product_category
ORDER BY quarterly_revenue DESC;
```

#### Refresh Strategy Comparison

| Approach              | How                                           | Late Data Handling                 |
|-----------------------|-----------------------------------------------|------------------------------------|
| Full refresh (nightly)| Truncate + reinsert entire table              | Perfect, but slow for large tables |
| Incremental (rolling window) | Recompute last N days of partitions  | Handles late data up to N days     |
| Streaming micro-batch | Flink / Spark Streaming writes to agg table   | Near real-time, complex setup      |
| Materialized views    | `REFRESH MATERIALIZED VIEW CONCURRENTLY`      | DB-managed, non-blocking refresh   |

> **Tip:** For a rolling 2-month incremental refresh, late-arriving data within 60 days is automatically corrected on the next run. Anything older is considered "closed" — document this SLA explicitly for business stakeholders.

---

## Section 5 — Distributed Systems & Integration

---

### Q12: Heterogeneous Data Sources — What They Are and How to Handle Them

#### Concept

**Heterogeneous data sources** means data that differs across two or more of these dimensions:

| Dimension            | Example of Heterogeneity                                          |
|----------------------|-------------------------------------------------------------------|
| **Format**           | One source is JSON, another is CSV, another is Avro              |
| **Structure**        | One source is relational (tables), another is document (nested)  |
| **Semantics**        | "customer_id" in CRM ≠ "user_id" in web events — same entity     |
| **Latency**          | One source is a batch file (daily), another is a real-time stream|
| **Quality**          | One source has nulls, another has inconsistent date formats      |
| **Scale**            | One source is 10K rows/day, another is 100M events/day           |

In practice, nearly every data engineering project involves heterogeneous sources — a CRM database, a web event stream, a payment API, a legacy CSV export, and a third-party data feed all feeding one warehouse.

#### Common Source Types and Integration Challenges

| Source Type         | Format       | Typical Challenge                                    | Integration Pattern                         |
|---------------------|--------------|------------------------------------------------------|---------------------------------------------|
| RDBMS (MySQL, PG)   | Structured   | Schema drift, large tables, no delete tracking       | CDC (Debezium) or snapshot + `MERGE`        |
| REST API (Stripe)   | JSON         | Pagination, rate limits, nested objects, retries     | Polling with cursor, flatten on ingest      |
| Kafka stream        | Avro / JSON  | Out-of-order events, schema registry, exactly-once   | Consumer group + watermarks                 |
| CSV / flat files    | CSV / TSV    | No enforced types, encoding issues, varying headers  | Schema-on-read with validation layer        |
| XML / EDI feeds     | XML          | Deep nesting, namespaces, schema variations          | XPath extraction + normalization            |
| SaaS exports        | JSON / XLSX  | Irregular schedules, format changes without notice  | Versioned raw landing + schema diffing      |

#### Schema-on-Read vs Schema-on-Write

```python
# ─── SCHEMA-ON-WRITE (traditional DWH approach) ───────────────────────────────
# Define a strict schema upfront. Data is validated and rejected if it doesn't conform.
# Pros: Clean, fast queries    Cons: Brittle when source schema changes

CREATE TABLE events_strict (
    event_id   VARCHAR(36) NOT NULL,
    user_id    BIGINT      NOT NULL,
    event_ts   TIMESTAMP   NOT NULL,
    amount     DECIMAL(10,2)
);
-- Inserting JSON with an unknown field → ERROR or silent drop


# ─── SCHEMA-ON-READ (data lake approach) ──────────────────────────────────────
# Store raw data as-is; apply schema interpretation at query time.
# Pros: Flexible, future-proof    Cons: Slower queries, deferred quality issues

import json

def ingest_raw(raw_payload: str, source: str, landed_at: str):
    """Store JSON as-is in the raw lake. No schema enforcement at ingest."""
    return {
        "source"      : source,
        "landed_at"   : landed_at,
        "raw_payload" : raw_payload,       # store entire JSON blob
    }

def read_with_schema(df):
    """Apply schema at read time using .get() with defaults."""
    return df["raw_payload"].apply(lambda r: {
        "event_id" : r.get("id") or r.get("event_id"),     # handle two naming conventions
        "user_id"  : r.get("user_id") or r.get("userId"),  # camelCase vs snake_case
        "amount"   : float(r.get("amount_cents", 0)) / 100,
    })
```

#### Semantic Harmonization (Entity Resolution)

A core challenge in heterogeneous integration: the **same real-world entity** has different keys across systems.

```sql
-- CRM uses customer_id, web events use anonymous user_id,
-- payments use stripe_customer_id. The identity_map links them.

CREATE TABLE identity_map (
    canonical_id        VARCHAR(50) PRIMARY KEY,  -- internal universal ID
    crm_customer_id     VARCHAR(20),
    web_user_id         VARCHAR(50),
    stripe_customer_id  VARCHAR(30),
    email_hash          CHAR(64),                 -- SHA-256 of email for matching
    created_at          TIMESTAMP,
    confidence_score    FLOAT                     -- 1.0 = deterministic match
);

-- Join across systems using canonical_id
SELECT
    im.canonical_id,
    c.customer_name,
    COUNT(e.event_id) AS web_events,
    SUM(p.amount)     AS total_payments
FROM identity_map     im
LEFT JOIN crm_customers c  ON im.crm_customer_id    = c.customer_id
LEFT JOIN web_events    e  ON im.web_user_id         = e.user_id
LEFT JOIN payments      p  ON im.stripe_customer_id  = p.customer_id
GROUP BY im.canonical_id, c.customer_name;
```

#### Integration Architecture (Lakehouse Pattern)

```
Sources:
  CRM (MySQL)        → CDC via Debezium        ─┐
  Web Events (Kafka) → Stream consumer          ├─→ Raw Landing Zone (S3/GCS)
  Stripe API         → Polling job              ─┘         │
  CSV Files          → File drop / SFTP        ─┘           │
                                                        Schema validation
                                                        + identity resolution
                                                             │
                                                     Unified Staging Layer
                                                     (common schema, clean types)
                                                             │
                                              ┌──────────────┴───────────────┐
                                         Star Schema                    Data Vault
                                        (Analytics DW)             (Audit / Compliance)
```

> **Tip:** The most underestimated challenge in heterogeneous integration is **semantic heterogeneity** — two fields named the same thing meaning different things (e.g., `created_at` = created in source system vs. created in your warehouse). Always document field lineage from source to warehouse.

---

## Quick Reference — Key Concepts Summary

| Concept               | One-Line Definition                                                              |
|-----------------------|----------------------------------------------------------------------------------|
| Grain                 | The single atomic event or entity each fact table row represents                  |
| Surrogate key         | Auto-generated integer PK that is independent of any source system key           |
| SCD Type 2            | Preserves full dimension history by inserting new rows on attribute change       |
| Data Vault Hub        | Stores business keys only — the identity anchors of the model                   |
| Data Vault Link       | Stores relationships between hubs — insert-only, no history                     |
| Data Vault Satellite  | Stores descriptive payload with timestamps — full history via new row inserts    |
| External merge sort   | Two-phase sort: create sorted runs → k-way merge; works with O(k) RAM            |
| Idempotency           | Running a pipeline multiple times produces the same result as running it once    |
| Schema-on-read        | Data stored raw; schema enforced only when queried                               |
| Semantic heterogeneity| Same real-world concept named or structured differently across source systems    |
| Partition pruning     | Query optimizer skips irrelevant partitions based on WHERE clause predicates     |
| Storage tiering       | Placing data in cheaper/slower storage as it ages and query frequency decreases  |
