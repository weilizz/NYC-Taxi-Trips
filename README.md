# NYC Yellow Taxi ELT Capstone

## Project overview

This project turns NYC Yellow Taxi trip records for January–March 2024 into tested analytics models and business reports. It uses Python to extract and load data, DuckDB as the warehouse, dbt to clean and model the data, and Airflow to orchestrate the pipeline.

**Team:** Carrie Teo Hui Xin, Inessya Fam Enn Xii, Tan Wei Li, 

## Business questions

The report models answer:

1. How many trips and how much `total_amount` revenue were there each month?
2. Which 10 pickup zones were busiest, and which pickup borough had the most revenue?
3. What were the busiest pickup hours and days of the week?
4. How did recorded tips differ by payment type?
5. How did airport trips compare with other trips by trip share, revenue share, average fare, and duration?

## Architecture

```text
NYC TLC Parquet trip files + taxi-zone lookup CSV
                 |
          Python extract
                 |
       Python load into DuckDB
          raw.trips / raw.zones
                 |
           dbt staging
        stg_trips / stg_zones
                 |
             dbt marts
       fct_trips / dim_zones
       business report models
                 |
       DuckDB report tables / CSV exports

Airflow orchestrates: extract -> load_raw -> dbt_run -> dbt_test
```

## Data sources

- NYC TLC Yellow Taxi trip data: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- NYC TLC taxi zone lookup: downloaded by the extract script from the TLC public data endpoint.
- The extract script is configured for `2024-01`, `2024-02`, and `2024-03`.

The raw inputs are stored in `data/`. The loader writes them to `warehouse/taxi.duckdb` as `raw.trips` and `raw.zones`.

## Data cleaning and quality

`stg_trips` filters the raw rows sequentially. Each rule is evaluated only against rows retained by the previous rule, so a row failing multiple rules is counted once, at its first failing rule.

| Rule | Rows before | Removed | Rows retained |
|---|---:|---:|---:|
| Raw input | — | — | 9,554,778 |
| Pickup timestamp from 2024-01-01 inclusive to 2024-04-01 exclusive | 9,554,778 | 21 | 9,554,757 |
| `trip_distance > 0` and `< 100` miles | 9,554,757 | 216,016 | 9,338,741 |
| `fare_amount > 0` and `total_amount > 0` | 9,338,741 | 127,750 | 9,210,991 |
| Duration from 1 to 180 minutes, inclusive | 9,210,991 | 33,378 | 9,177,613 |

These counts are from the loaded January–March 2024 raw dataset and the filters in `models/staging/stg_trips.sql`.

## Data model

- **Grain of `fct_trips`:** one row per trip record retained by `stg_trips`.
- `fct_trips.trip_id` is a generated row-number key.
- `pickup_zone_id` and `dropoff_zone_id` reference `dim_zones.zone_id`.
- **Grain of `dim_zones`:** one row per taxi zone, keyed by `zone_id`.

dbt tests cover key uniqueness/non-nullness and zone-key relationships.

## Report models

All models are in `taxi_dbt/models/marts/`.

| Business question/report | dbt model |
|---|---|
| Trips and revenue by month | `trips_by_month` |
| 10 busiest pickup zones | `top_pickup_zones` |
| Pickup trips and revenue by borough | `revenue_by_borough` |
| Trips by pickup hour | `trips_by_hour` |
| Trips by weekday, Monday through Sunday | `trips_by_day_of_week` |
| Recorded tips by payment type | `tips_by_payment` |
| Airport trips versus other trips | `airport_trips` |

The report tables are materialized in the DuckDB `main` schema. The CSV snapshots for dashboarding are in `dashboard_data/`.

## Run the pipeline

The commands below use the paths configured in this workshop environment. The capstone Airflow DAG also uses `/home/codespace/workshop/capstone` and `/home/codespace/venvs/dbt/bin`; adjust those paths in `dags/taxi_elt_dag.py` if running elsewhere.

### Run dbt

The raw tables must already exist in the warehouse. From the project root:

```bash
cd /home/codespace/workshop/capstone/taxi_dbt
~/venvs/dbt/bin/dbt build --profiles-dir .
```

`dbt build` builds the models and runs the configured data tests.

### Run the complete pipeline in Airflow

From a terminal:

```bash
cd /home/codespace/workshop/capstone
source airflow_env.sh
airflow standalone
```

Open Airflow at `http://localhost:8080` (or use the forwarded port in Codespaces), sign in with the local Airflow account, and trigger `taxi_elt_pipeline`. The task order is:

```text
extract -> load_raw -> dbt_run -> dbt_test
```

Keep the Airflow terminal running while using the web UI. Stop it with `Ctrl+C` when finished. Do not publish or commit Airflow passwords.

## View report results

After `dbt build`, query the warehouse from the capstone root. For example:

```bash
cd /home/codespace/workshop/capstone
~/venvs/dbt/bin/python -c "import duckdb; duckdb.connect('warehouse/taxi.duckdb', read_only=True).sql('SELECT * FROM trips_by_month').show()"
```

Replace `trips_by_month` with another model name from the report table above. The dashboard CSVs are snapshots; rebuild/export them again if the underlying data changes.

## Selected findings

From the current cleaned January–March 2024 data:

- March had the most trips (3,427,498) and the most `total_amount` revenue ($95,367,987.28).
- Midtown Center was the busiest pickup zone (441,006 trips).
- Manhattan had the most pickup-zone `total_amount` revenue ($190,706,786.28).
- 6 PM was the busiest pickup hour (664,953 trips); Thursday was the busiest weekday (1,486,625 trips).
- Airport trips were 9.47% of trips and 26.83% of `total_amount` revenue. Their average fare was $56.14 versus $14.75 for other trips.

These are descriptive results, not proof of what caused demand or revenue differences.

## Important caveats

- The analysis covers only January–March 2024; seasonal patterns should be checked against more months and years.
- `total_amount` is a revenue measure from the available trip data, not profit. Costs, fees, driver expenses, and other operating costs are not included.
- Cash is recorded as a payment method, but cash tips are generally not reliably recorded. The displayed cash tip values are incomplete and should not be interpreted as actual cash tipping or compared directly with card tips.
- Airport trips are counted once if either the pickup or dropoff zone is an airport, including trips with both endpoints at airports.
- Results reflect the staging filters documented above.

## Project structure

```text
capstone/
├── dags/taxi_elt_dag.py
├── data/                       # downloaded Parquet and zone lookup inputs
├── dashboard_data/             # report CSV snapshots for dashboarding
├── scripts/
│   ├── extract_taxi.py
│   └── load_raw.py
├── taxi_dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       └── marts/
├── warehouse/taxi.duckdb       # generated DuckDB warehouse
└── airflow_env.sh
```
