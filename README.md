# Hotel Bookings Lakehouse

This project processes hotel-booking data on Databricks using a medallion-style
pipeline. It incrementally ingests raw CSV files into Bronze, cleans and checks
the data in Silver, and creates booking and revenue tables in Gold. The
Databricks resources are defined as a Declarative Automation Bundle (DAB).

## Data flow

1. **Bronze ingestion** — The `Bronze Layer Ingestion Job` runs
   `SrcParameters.ipynb` to provide source names, then runs
   `01_bronze_ingestion.ipynb` for each source. The ingestion code uses
   Auto Loader to read CSV files and append them as Delta data. For the
   configured `hotel_bookings` source, raw files are read from
   `/Volumes/hotel_project/raw_data/raw_files/hotel_bookings/`; schema,
   checkpoint, and data are stored under
   `/Volumes/hotel_project/bronze/bronzevolume/hotel_bookings/`.
2. **Silver transformation** — The `DLT_SILVER_LAYER` pipeline reads Bronze
   data, casts booking columns to their intended types, removes rescued data,
   and adds a modification timestamp. Its expectations drop records with
   invalid cancellation flags, guest counts, or ADR values. The resulting
   table is `silver.silver_hotel_bookings`.
3. **Gold transformation** — The `DLT_GOLD_LAYER` pipeline reads the Silver
   table, derives arrival dates, total nights, and estimated room revenue, then
   produces:
   - `gold_hotel_booking_summary` — total, canceled, and non-canceled bookings
     by hotel.
   - `gold_monthly_booking_trends` — booking and cancellation counts by hotel
     and arrival month.
   - `gold_monthly_room_revenue` — estimated room revenue, checked-out booking
     counts, and average ADR by hotel and arrival month.

The `Hotel Bookings BSG Job` orchestrates the stages in order: Bronze job,
Silver pipeline, then Gold pipeline.

## Repository layout

```text
k_hotels_dab/
├── databricks.yml          # Bundle name and dev/prod targets
├── resources/
│   ├── jobs/               # Bronze ingestion and end-to-end jobs
│   └── pipelines/          # Silver and Gold pipeline definitions
├── src/
│   ├── bronze/             # Auto Loader ingestion helpers
│   ├── DLT_SILVER_LAYER/   # Silver pipeline and transformations
│   ├── DLT_GOLD_LAYER/     # Gold pipeline and transformations
│   └── notebooks/          # Bronze job notebooks
└── tests/
    ├── bronze/             # Source validation and path tests
    ├── silver/             # Casting, cleaning, and expectation tests
    └── gold/               # Derived fields and aggregation tests
```

## Run tests

The tests use Python 3.12 and local Spark. Install Java 17, then from this
directory install the test dependencies and run pytest:

```bash
python -m pip install pytest pyspark==3.5.0
python -m pytest tests/ -v
```

The tests cover Bronze source-name validation and path generation, Silver
transformations and data-quality expectations, and Gold derived values and
aggregations.

## Deploy to Databricks

The bundle has `dev` and `prod` targets in [`databricks.yml`](databricks.yml).
Authenticate the Databricks CLI to the target workspace, then run these
commands from this directory:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

To deploy and run production instead, use:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod Hotel_Bookings_BSG_Job
```

The production target is configured with production mode, the `hotel_project`
catalog, the `prod` schema, and a workspace root path specified in
[`databricks.yml`](databricks.yml). Confirm these values and the Databricks
workspace before deploying to production.

## CI/CD

The GitHub Actions workflow at
[`../.github/workflows/ci-cd.yml`](../.github/workflows/ci-cd.yml) runs the
pytest suite for pushes to `dev` and `main`, and for pull requests targeting
either branch. On a push to `main`, it deploys the `prod` bundle and runs
`Hotel_Bookings_BSG_Job` only after the test job succeeds.

For CI deployment, configure a GitHub environment named `production` and add
these environment secrets:

- `DATABRICKS_HOST` — the Databricks workspace URL.
- `DATABRICKS_TOKEN` — a token authorized to deploy and run the production
  bundle.

The workflow uses these secrets for Databricks CLI authentication; do not put
tokens in the repository.
