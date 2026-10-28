# 🏙️ Smart City Analytics — UrbanIQ Pipeline

An end-to-end data engineering and machine learning pipeline for urban transit and infrastructure monitoring. It extracts data from multiple sources, cleans and transforms it, loads it into a MySQL data warehouse, runs SQL analytics, trains an infrastructure-health prediction model, and orchestrates everything with **Apache Airflow**.

**Authors:** Hala AlJafary · Hiba Daher · Shahed Abu Baidar

---

## 📌 Overview

| Stage | What it does |
|---|---|
| **Extract** | Scrapes city transit indicators from Numbeo and pulls sensor readings from a REST API (with a synthetic-data fallback) |
| **Transform** | Cleans the data, engineers features, validates data quality |
| **Load** | Loads three tables into the `UrbanIQ_Warehouse` MySQL database (idempotent truncate-and-load) |
| **Analyze** | Three SQL analytics queries exported as a daily text report |
| **Predict** | Logistic Regression + SMOTE to predict sensor status (`OK` / `FAIL`) |
| **Orchestrate** | An Airflow DAG that runs the whole pipeline every 6 hours |

---

## 🗺️ Pipeline Flow

```
extract_transit_data ─┐
                      ├─► transform_urban_data ─► load_data_to_warehouse ─┬─► generate_city_insights
extract_sensor_data  ─┘                                                   └─► train_health_model
```

- The two extraction tasks run **in parallel**.
- Transformation only starts after both extractions succeed, and loading only starts after transformation, so no raw or unchecked data reaches the warehouse.
- Insights and model training run **in parallel** on the loaded data.

---

## 📂 Repository Structure

```
.
├── Smart-City-Analytics.ipynb          # Full pipeline code (ETL, analytics, ML, Airflow DAG)
├── Smart-City-Analytics-Report.pdf     # Project report
└── README.md
```

---

## 🔹 Part 1 — Multi-Source ETL

### Extract

**A. Public transit data (Numbeo).** Numbeo's tables do not expose transit-specific columns, so the following were **engineered from related indicators** (Traffic Commute Time, Pollution, Safety, Quality of Life, Cost of Living, Property Price to Income Ratio) using MinMax normalization and weighted combinations:

| Derived column | Based on |
|---|---|
| `Transit_Score` | Commute time (50%, inverted) + Pollution (30%, inverted) + Safety (20%) |
| `Avg_Delay_Minutes` | Commute time index |
| `Ridership_Millions` | Commute time, quality of life, cost of living |
| `Annual_Budget_USD` | Estimated ridership + cost of living |
| `System_Age_Years` | Property price pressure + commute time |

Transit **lines and stations** (`transit_lines`) are **synthetic**, generated with realistic ranges so the relational model and downstream analytics can be built.

**B. Infrastructure sensor data (API).** The pipeline first tries a live API request (with timeout and `raise_for_status()`). If the API is unreachable, it falls back to a **synthetic sensor dataset** (`sensor_id`, `timestamp`, `temperature`, `humidity`, `status`, `vibration`) generated with a fixed seed (`np.random.seed(42)`) for reproducibility.

> ⚠️ Because of the engineered and synthetic data, the results in this project demonstrate the **pipeline**, not real-world transit measurements.

### Transform
- **Data-quality check** on the raw sensor data: missing-value ratio (> 20%), out-of-range temperature (−10 to 50 °C), abnormal vibration (> 100) — raises a simulated alert if violated.
- Cleans names/countries, fills missing budget values, and computes `Efficiency_Score` (ridership ÷ budget).
- Buckets average delay into `Short` / `Average` / `Tall`.
- Sensor data: forward-fill nulls, parse timestamps, add `hour` and `day_of_week`, drop invalid (negative) vibration values, and scale `vibration` and `temperature` to [0, 1].

### Load
Loads three tables into MySQL via SQLAlchemy:

| Table | Description |
|---|---|
| `transit_systems` | City-level transit metrics (primary key `City_ID`) |
| `transit_lines` | Line-level data (foreign key → `transit_systems`) |
| `sensor_logs` | Cleaned sensor readings |

Loading uses a **truncate-and-load** strategy (tables are cleared before inserting), so re-running the pipeline never creates duplicates.

---

## 🔹 Part 2 — Analytics & Predictive Modeling

### SQL analytics
Results are written to `city_insights_report_<date>.txt`:

1. **Top 5 transit-efficiency cities** — ridership ÷ annual budget.
2. **Average delay by line type** — joins `transit_lines` with `transit_systems`.
3. **High-performing but aging systems** — transit score and system age both above the 75th percentile (dynamic thresholds, not hard-coded).

### Infrastructure health prediction
- **Target:** sensor `status` (`OK` = 1, `FAIL` = 0)
- **Features:** `sensor_id`, `temperature`, `humidity`, `vibration`, `hour`
- **Class imbalance:** handled with **SMOTE** (training set only)
- **Model:** Logistic Regression (interpretable and efficient)
- **Metrics:** accuracy, precision, recall, F1
- **Alerting:** simulated alert if accuracy drops below 40%
- The trained model is saved to `model.joblib` and reloaded to verify it produces identical predictions.

---

## 🔹 Part 3 — Orchestration with Apache Airflow

DAG: `urbaniq_monitoring_pipeline`

| Setting | Value |
|---|---|
| Schedule | Every 6 hours (`0 */6 * * *`) |
| Start date | 2025-12-19 |
| `catchup` | `False` |
| Retries | 3, with a 5-minute delay |
| Failure e-mails | Enabled (`email_on_failure=True`) |
| `depends_on_past` | `False` |

---

## 🛠️ Tech Stack

Python · pandas · NumPy · scikit-learn · imbalanced-learn · SQLAlchemy · PyMySQL · MySQL · Apache Airflow · Requests · Jupyter

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- A running **MySQL** server on `localhost:3306`
- (Part 3 only) Apache Airflow — on Windows use **WSL2** or **Docker**

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>
```

### 2. Install dependencies
```bash
pip install pandas numpy requests lxml scikit-learn imbalanced-learn joblib sqlalchemy pymysql apache-airflow
```

### 3. Configure the database
Open the notebook and edit the settings cell near the top:

```python
DB_SERVER_URL = "mysql+pymysql://<user>:<password>@localhost:3306"
```

The `UrbanIQ_Warehouse` database and its tables are created automatically.

### 4. Run
```bash
jupyter notebook Smart-City-Analytics.ipynb
```
Run the cells from top to bottom. Parts 1 and 2 work without Airflow; Part 3 needs Airflow installed.

---

## ⚠️ Limitations & Future Work

- Transit metrics (ridership, budget, system age, lines) are **estimated or synthetic**; connecting a real transit data source would make the analytics meaningful.
- The sensor API may be unavailable; the pipeline then runs on synthetic fallback data.
- In the Airflow DAG, tasks are defined with their dependencies, but data is not yet passed between tasks (e.g., via XCom or intermediate storage).
- The health model uses a small dataset; adding more sensor history and evaluating with cross-validation would give more reliable metrics.
- Move database credentials and alert e-mails to environment variables / Airflow connections.

---

## 📄 License

Add your preferred license here (e.g., MIT).
