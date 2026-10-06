# Airflow → Redshift Data Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Amazon Redshift](https://img.shields.io/badge/Amazon%20Redshift-8C4FFF?style=for-the-badge&logo=amazonredshift&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

![Tasks](https://img.shields.io/badge/DAG%20tasks-11-blue?style=flat-square)
![Operators](https://img.shields.io/badge/custom%20operators-5-orange?style=flat-square)
![Tables](https://img.shields.io/badge/Redshift%20tables-7-purple?style=flat-square)
![Schedule](https://img.shields.io/badge/schedule-hourly-green?style=flat-square)

> **An hourly, self-validating ETL pipeline that loads JSON from S3 into a Redshift star schema, orchestrated by Apache Airflow with reusable custom operators.**

![Pipeline DAG](docs/images/pipeline_dag.svg)

---

## 🔍 What This Pipeline Does

**Orchestration, not just a script.**

A music streaming app ("Sparkify") stores user activity logs and song metadata as raw JSON in S3. Running the ETL by hand works once. This project automates it end to end: it runs every hour, retries on failure, runs independent loads in parallel, and **fails loudly if the data comes out empty**.

Each run:
1. Creates the warehouse schema (idempotent `CREATE TABLE IF NOT EXISTS`).
2. Stages raw JSON from S3 into Redshift with `COPY`, with events and songs loading **in parallel**.
3. Builds the `songplays` fact table by joining events to songs.
4. Loads **4 dimension tables in parallel** through SubDAGs, with optional truncate-and-load.
5. Runs a **data quality gate** on every final table before the run is marked successful.

---

## 🧩 Custom Operators

All business logic sits in 5 reusable operators, so the DAG file only wires tasks together.

| | Operator | What it does |
|---|---|---|
| 🏗️ | `CreateTableOperator` | Runs the bundled DDL (`create_tables.sql`) to build staging, fact and dimension tables |
| 📥 | `StageToRedshiftOperator` | Templated `COPY` from any S3 prefix; supports a JSONPaths file or `auto`; pulls AWS keys from an Airflow connection |
| ⭐ | `LoadFactOperator` | Append-only `INSERT … SELECT` into the fact table |
| 🗂️ | `LoadDimensionOperator` | Dimension load with a **truncate-insert** switch (`delete_load=True`) |
| ✅ | `DataQualityOperator` | Row-count check across a list of tables; raises `ValueError` so the run fails |

---

## 🔀 DAG Design

| | Setting | Value | Why |
|---|---|---|---|
| ⏰ | Schedule | `0 * * * *` (hourly) | Keeps the warehouse fresh |
| 🔗 | `depends_on_past` | `True` | A run never starts if the previous hour failed |
| 🚦 | `max_active_runs` | `1` | Prevents overlapping loads into the same tables |
| 🔁 | Retries | 1, after 5 min | Absorbs transient S3 or Redshift errors |
| ⚡ | Parallelism | 2 staging + 4 dimension tasks | Shorter end-to-end runtime |
| 🧱 | SubDAGs | One factory, 4 dimension loads | Avoids copy-pasting the same task 4 times |

---

## ⭐ Data Model

![Star schema](docs/images/star_schema.svg)

| Table | Type | Built from |
|---|---|---|
| `staging_events` | Staging | `COPY` of `log-data/` using `log_json_path.json` |
| `staging_songs` | Staging | `COPY` of `song_data/` (`auto`) |
| `songplays` | **Fact** | Events (`page='NextSong'`) ⨝ songs on title + artist + duration. Key is `md5(sessionid ‖ start_time)` |
| `users` | Dimension | Distinct users from events |
| `songs` | Dimension | Distinct songs |
| `artists` | Dimension | Distinct artists |
| `time` | Dimension | `start_time` split into hour, day, week, month, year and dayofweek |

---

## 🛠️ What I Built and Changed

- **Designed the DAG**: task dependencies, parallel staging and dimension branches, and a data quality gate at the end.
- **Wrote 5 custom Airflow operators** and a SubDAG factory, packaged as an Airflow plugin.
- **Wrote the SQL transforms**: epoch-ms to timestamp conversion, an md5 surrogate key, and time-dimension extraction.
- **Made it portable**: the DDL used to be read from a hardcoded Udacity workspace path. It's now bundled as `plugins/helpers/create_tables.sql` and resolved relative to the operator.
- **Made the source bucket configurable** through the `S3_BUCKET` environment variable.
- **Added a one-command local environment** (`docker-compose.yml`) with catch-up disabled, so it doesn't backfill years of hourly runs.
- **Included a sample dataset** (`data/`) ready to upload to S3, with macOS `.DS_Store` files removed because they break Redshift `COPY`.
- **Added infrastructure as code**: a CloudFormation template for running Airflow on EC2 with an RDS metadata DB.

---

## ⚡ Quick Start

```bash
# 1. Clone
git clone https://github.com/Khushipatel27/airflow-redshift-data-pipeline.git
cd airflow-redshift-data-pipeline

# 2. Upload sample data to your S3 bucket (same region as Redshift)
aws s3 sync data/ s3://<your-bucket>/

# 3. Configure the bucket name
cp .env.example .env            # set S3_BUCKET=<your-bucket>

# 4. Start Airflow (Docker)
docker compose up -d
```

Airflow opens at **http://localhost:8080**.

5. Add the `aws_credentials` and `redshift` connections by following [Setup_Redshift_Connection_Airflow.md](Setup_Redshift_Connection_Airflow.md).
6. Turn `udac_example_dag` **On**, then click **▶ Trigger DAG**. All 11 tasks should go green.
7. Verify the load in the Redshift query editor:
   ```sql
   SELECT COUNT(*) FROM songplays;
   SELECT * FROM users LIMIT 5;
   ```
8. Clean up: run `docker compose down` and delete your Redshift workgroup so you aren't charged.

> 💡 **Free check, no AWS needed:** `docker exec airflow airflow list_tasks udac_example_dag --tree` confirms the DAG and plugins load.

---

## 📚 Dataset

| | Source | Contents |
|---|---|---|
| 🎵 | `data/song_data/` | 71 JSON files from the [Million Song Dataset](http://millionsongdataset.com/): song, artist, duration, year |
| 📝 | `data/log-data/` | 30 daily JSON logs from an [event simulator](https://github.com/Interana/eventsim): **8,026 events**, **6,801 song plays**, 98 users |
| 🗺️ | `data/log_json_path.json` | JSONPaths file that maps log fields to `staging_events` columns |

---

## 📁 Project Structure

```
airflow-redshift-data-pipeline/
├── dags/
│   ├── udac_example_dag.py              ← main DAG: 11 tasks, hourly schedule
│   └── sparkify_dimension_subdag.py     ← SubDAG factory for dimension loads
│
├── plugins/
│   ├── operators/
│   │   ├── create_table.py              ← CreateTableOperator
│   │   ├── stage_redshift.py            ← StageToRedshiftOperator (S3 COPY)
│   │   ├── load_fact.py                 ← LoadFactOperator
│   │   ├── load_dimension.py            ← LoadDimensionOperator (truncate-insert)
│   │   └── data_quality.py              ← DataQualityOperator
│   └── helpers/
│       ├── sql_queries.py               ← INSERT … SELECT transforms
│       └── create_tables.sql            ← Redshift DDL (7 tables)
│
├── data/                                ← sample dataset to upload to S3
├── docs/images/                         ← diagrams
├── docker-compose.yml                   ← local Airflow 1.10 (one command)
├── .env.example                         ← S3_BUCKET template
├── Airflow_CloudFormation.yaml          ← Airflow on EC2 + RDS (IaC)
├── Airflow_Livy_Setup_CloudFormation.md ← AWS deployment guide
└── Setup_Redshift_Connection_Airflow.md ← connection setup
```

---

## ⚠️ Notes

- The code targets **Airflow 1.10**, which is what the provided `docker-compose.yml` runs. Airflow 2.x would need updated import paths.
- Credentials are never stored in the repo. They live in Airflow connections, and `.env` is gitignored.
- Redshift costs money while it's running, so always delete the workgroup or cluster after testing.

---

<p align="center">Built with ❤️ by <b>Khushi Patel</b></p>
