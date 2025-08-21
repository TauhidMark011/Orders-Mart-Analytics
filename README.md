📦 Orders-Mart-Analytics :- 
This project demonstrates the end-to-end design and implementation of an Analytical Orders Mart using Snowflake, PostgreSQL, and dbt. The goal is to design a scalable, reliable, analytics-ready pipeline that ingests raw data, applies transformations via dbt, and produces fact & dimension tables for BI use-cases. A simple BI demo is created with Google Looker Studio. While Snowflake was the core data warehouse for this project, PostgreSQL was introduced as a secondary environment for data validation and to showcase cross-platform SQL workflows. The raw data was first staged and loaded into Snowflake, after which dbt transformations were applied to build fact and dimension tables.

🔎 Project Highlights (short)

- Data warehouse: Snowflake (raw → staging → marts), Data modeling (Star Schema) for efficient analytics.
- On-prem / local relational copy: PostgreSQL was used as the local staging and validation layer in our pipeline, before pushing data into Snowflake. (staging/curated for experimentation) also used for Data Validation Script, Referential Integrity Checks.
- Transformations & documentation: dbt (models, tests, sources, exposures, docs), dbt transformations for modular, version-controlled pipelines.  
- BI demo: Google Looker Studio (connected to Snowflake), Analytics filters and slicing (Customer & Date range dimensions) to provide insights. 
- Data quality: dbt tests + source freshness checks. 
- Version control: Version control with GitHub for collaborative and production-ready development.
- Exports: CSVs generated for Looker Studio if preferred.

  🎯 Objectives & Purpose
- Build a scalable and reliable analytics mart for retail orders, Created dimension and fact tables to enable slicing by customers, orders, and time. Ensure reusability and automation using dbt. Demonstrate best practices in data engineering: version control, environment isolation, and structured documentation.

✅ Achievements & Progress
1) Data Warehouse Setup
- Configured Snowflake environment for storing raw, staging, and analytics models.
2) Data Modeling & Star Schema
- Designed dimension tables: dim_customer, dim_product, dim_date, etc. Built fact tables: fct_orders, fct_payments.
3) dbt Implementation
- Implemented modular transformations with stg_, dim_, and fct_ layers.
- Added tests, sources, and documentation within dbt.
- Configured date range dimension filtering (ORDER_DATE) and customer-level slicing (CUSTOMER_KEY).
4) Version Control & GitHub Integration
- Established GitHub repository: Orders-Mart-Analytics.
- Added .gitignore to securely exclude credentials (e.g., set_env.ps1).
- Successfully pushed and version-controlled all dbt project files.
5) Analytics & Insights Ready
- Star schema and dbt models now allow slicing/filtering: By Customer (via CUSTOMER_KEY), By Date Range (via ORDER_DATE).

Provides a solid foundation for BI dashboards and advanced analytics.

🧭 What we built (high level)
1) Raw ingestion (Snowflake)
- Raw tables: orders_raw, customers_raw, products_raw (in ORDERS_DB.RAW or your raw DB)
- LOAD_TIMESTAMP column available for freshness checks.

2) Staging (dbt models: stg_*)
- Cleaned, canonical columns, removed ingestion metadata fields, camel/snake case normalization.

3)Marts (dbt models: dim_*, fct_*)
- dim_customers, dim_products (dimension tables), fct_orders (fact table at natural grain: one row per order_id + product_id), Unit price cast to NUMBER(10,2) and order_amount in NUMBER(12,2).

4) Data validation & quality
- dbt tests (not_null, unique, relationships) — all tests passed in your run (29/29). Source freshness checks for raw tables.

5) Documentation & lineage
- exposures.yml for dashboard lineage. dbt docs generate + dbt docs serve for model catalog & lineage graph (used in interview demo).

6) BI demo(📊 Dashboard design)
- Google Looker Studio dashboard (3 charts + filters): Time series: total sales over time, Bar chart: sales by product. Table: order lines. Filters: Customer (CUSTOMER_KEY), Date range (ORDER_DATE).

📁 Repo structure (typical)

orders-mart-analytics/
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   ├── marts/
│   │   ├── dim_customers.sql
│   │   ├── dim_products.sql
│   │   └── fct_orders.sql
│   ├── schema.yml         # model & tests (marts)
│   ├── sources.yml        # sources + freshness
│   └── exposures.yml
├── packages.yml
├── dbt_project.yml
  ├── profiles.yml (UNCOMMIT)
├── set_env.ps1 (UNCOMMIT)   # helper for local env (gitignored)
├── docs/ (screenshots, dashboards images)
├── sql/ (ad-hoc queries for Looker Studio)
└── README.md

📋 How to run — step-by-step (developer instructions)
Pre-reqs :-
- Python + venv, dbt-core + adapter (dbt-snowflake), snowflake account, Snowflake role with privileges, dbt profile configured for Snowflake (see sample below).
- Activate your venv before running dbt:
Windows PowerShell: .\.venv\Scripts\Activate.ps1 (you may need Set-ExecutionPolicy -Scope CurrentUser RemoteSigned once).
- Place your Snowflake credentials into your local ~/.dbt/profiles.yml or use session env vars — do not commit the profile.
1) Install packages
# from repo root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install dbt-core==1.10.8 dbt-snowflake==1.10.0  # use same versions you used
# then
dbt deps
packages.yml should include dbt_utils if you use its macros:
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.9.6", "<2.0.0"]

2)Ensure your dbt profile is configured (example)
~/.dbt/profiles.yml (example for Snowflake — replace with your values)
orders_mart_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>           # e.g., xy12345.us-east-1
      user: <SNOWFLAKE_USER>
      password: <SNOWFLAKE_PASSWORD>
      role: ACCOUNTADMIN
      database: ORDERS_DB
      warehouse: COMPUTE_WH
      schema: ORDERS_MART
      threads: 4
      client_session_keep_alive: False

3) (Optional) set environment variables script
Create set_env.ps1 with SNOWFLAKE_USER, SNOWFLAKE_PASSWORD etc for local dev, but gitignore it. You used this pattern already.

4) Build order (commands)
Run these from project root with the venv active:
Install dependencies: 3) (Optional) set environment variables script

Create set_env.ps1 with SNOWFLAKE_USER, SNOWFLAKE_PASSWORD etc for local dev, but gitignore it. You used this pattern already.

4) Build order (commands)
Run these from project root with the venv active:
a.Install dependencies: dbt deps
b.Validate config & parse: dbt debug, dbt parse.
c.Check source freshness (if you keep freshness checks enabled and data is present): dbt source freshness
d.Build staging models: dbt run --select stg_customers stg_orders stg_products
e.Build marts (dimensions + facts): dbt run --select dim_customers dim_products fct_orders
f.Run tests: dbt test
g.Generate docs & view: Generate docs & view:


