# 🧑‍💻 Dhyey Patel

## 🛠️ DBT + Snowflake: Customer Lifetime Value Pipeline

This project demonstrates a full modern data pipeline using [dbt](https://www.getdbt.com/) and [Snowflake](https://www.snowflake.com/) to model and calculate **Customer Lifetime Value (LTV)** and engagement metrics from raw CSV inputs.

---

### 📦 Project Structure
dbt-oncourse-prep/
├── models/
│ ├── staging/
│ │ ├── stg_customers.sql
│ │ ├── stg_orders.sql
│ │ ├── stg_events.sql
│ │ └── sources.yml
│ └── marts/
│ └── customer_ltv.sql
├── dbt_project.yml
└── README.md


---

### 🔍 Use Case

We calculate each customer's:
- 🛒 Total orders
- 💵 Lifetime value (LTV)
- 📅 First and most recent order
- 📈 Total and unique event interactions
- 📱 Last active event timestamp

---

### 🧠 Key Concepts Covered

| Feature            | Description                                       |
|--------------------|---------------------------------------------------|
| `sources.yml`      | Tracks freshness + quality of raw Snowflake tables |
| Staging Models     | Use `ref()` and `source()` with Jinja + macros     |
| Mart Model         | Joins across `orders`, `customers`, and `events`  |
| Schema Tests       | Validates `not_null`, `unique`, and relationships |
| Snowflake Targets  | Outputs models into `dbt_dpatel_analytics` schema |
| CLI Workflows      | Built + tested using `dbt run` and `dbt test`     |

---

### ✅ Data Quality Tests Included

- Unique + not-null checks on all primary keys
- Schema-defined tests in `models/staging/schema.yml`
- Full test suite runs with `dbt test`

---

### 🚀 How to Run Locally

```bash
# Set up virtual environment
python -m venv dbt_venv
.\dbt_venv\Scripts\activate

# Install dbt
pip install dbt-snowflake

# Run the pipeline
dbt debug       # check connection
dbt run         # build models
dbt test        # validate with schema tests

 Future Improvements
Add segmentation models (e.g. High, Mid, Low LTV)

Add retention curves using event timestamps

Integrate with Airflow / Prefect for orchestration
