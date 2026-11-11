import argparse
import csv
import hashlib
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values, register_uuid

register_uuid()

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "source_talento")
DB_USER = os.getenv("SRC_WRITER_USER", os.getenv("POSTGRES_USER", "src_writer"))
DB_PASS = os.getenv("SRC_WRITER_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))
DATA_SOURCES_DIR = Path(
    os.getenv("DATA_SOURCES_DIR", str(BASE_DIR / "01_source_system" / "data_sources"))
)

BATCH_SIZE = 5000

DATASETS_CATALOG = [
    {
        "dataset_name": "cost_of_living",
        "file_name": "(Datos Externos)  costo de vida global -  Kaggle.csv",
        "table_name": "src.cost_of_living_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 1,
        "temporal_type": "static",
        "time_field": None,
        "series_keys": [],
        "value_fields": [],
    },
    {
        "dataset_name": "foreign_affiliates_gender",
        "file_name": "(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social.csv",
        "table_name": "src.foreign_affiliates_gender_raw",
        "encoding": "latin-1",
        "delimiter": ";",
        "header_lines_count": 3,
        "temporal_type": "monthly",
        "time_field": "payload[0]",
        "series_keys": [1],
        "value_fields": "auto",
        "value_start": 2,
    },
    {
        "dataset_name": "eurostat_hitech_employment",
        "file_name": "(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv",
        "table_name": "src.eurostat_hitech_employment_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 1,
        "temporal_type": "annual",
        "time_field": "TIME_PERIOD",
        "series_keys": ["freq", "category", "isco08", "age", "unit", "geo"],
        "value_fields": ["OBS_VALUE"],
    },
    {
        "dataset_name": "epa_labor_market",
        "file_name": "(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv",
        "table_name": "src.epa_labor_market_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 2,
        "temporal_type": "static",
        "time_field": None,
        "series_keys": [],
        "value_fields": [],
    },
    {
        "dataset_name": "istac_employment_cnae",
        "file_name": "(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv",
        "table_name": "src.istac_employment_cnae_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 2,
        "temporal_type": "quarterly",
        "time_field": "header_row_1",
        "series_keys": [0, 1],
        "value_fields": "auto",
        "value_start": 2,
    },
    {
        "dataset_name": "istac_employment_divisions",
        "file_name": "(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv",
        "table_name": "src.istac_employment_divisions_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 1,
        "temporal_type": "quarterly",
        "time_field": "header_row_1",
        "series_keys": [0],
        "value_fields": "auto",
        "value_start": 1,
    },
    {
        "dataset_name": "istac_employment_status",
        "file_name": "(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv",
        "table_name": "src.istac_employment_status_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 2,
        "temporal_type": "quarterly",
        "time_field": "header_row_1",
        "series_keys": [0, 1],
        "value_fields": "auto",
        "value_start": 2,
    },
    {
        "dataset_name": "foreign_affiliates_cnae",
        "file_name": "(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv",
        "table_name": "src.foreign_affiliates_cnae_raw",
        "encoding": "utf-8-sig",
        "delimiter": ";",
        "header_lines_count": 2,
        "temporal_type": "monthly",
        "time_field": "payload[0]",
        "series_keys": [1],
        "value_fields": "auto",
        "value_start": 2,
    },
    {
        "dataset_name": "tech_salaries",
        "file_name": "(Datos Externos) Salarios Tecnológicos - Kaggle.csv",
        "table_name": "src.tech_salaries_raw",
        "encoding": "utf-8-sig",
        "delimiter": ",",
        "header_lines_count": 1,
        "temporal_type": "annual",
        "time_field": "work_year",
        "series_keys": [
            "experience_level",
            "employment_type",
            "job_title",
            "salary_currency",
            "employee_residence",
            "company_location",
            "company_size",
        ],
        "value_fields": ["salary", "salary_in_usd", "remote_ratio"],
    },
    {
        "dataset_name": "emigration_pere",
        "file_name": "(Datos Externos) Datos de Emigración - INE.csv",
        "table_name": "src.emigration_pere_raw",
        "encoding": "utf-8-sig",
        "delimiter": ";",
        "header_lines_count": 1,
        "temporal_type": "annual",
        "time_field": "Periodo",
        "series_keys": [
            "País de residencia (con 1.000 o más residentes)",
            "Total Nacional",
            "Comunidades y Ciudades Autónomas",
            "Provincias",
            "Sexo",
            "Lugar de nacimiento",
        ],
        "value_fields": ["Total"],
    },
]


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_value_fields(dataset, header):
    value_fields = dataset["value_fields"]
    if value_fields == "auto":
        return list(range(dataset["value_start"], len(header)))
    return value_fields


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def get_existing_hash(cur, dataset_name):
    cur.execute(
        "SELECT sha256 FROM src.dataset_files WHERE dataset_name = %s",
        (dataset_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def load_dataset(conn, dataset, force=False):
    dataset_name = dataset["dataset_name"]
    table_name = dataset["table_name"]
    file_path = DATA_SOURCES_DIR / dataset["file_name"]

    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el fichero: {file_path}")

    file_hash = sha256_file(file_path)

    with conn.cursor() as cur:
        existing_hash = get_existing_hash(cur, dataset_name)

    if existing_hash == file_hash and not force:
        print(f"[SKIP] {dataset_name}: ya cargado con el mismo SHA-256")
        return

    if existing_hash is not None and existing_hash != file_hash and not force:
        raise RuntimeError(
            f"{dataset_name}: el fichero ha cambiado. Usa --force para recargarlo."
        )

    load_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    header_lines = []
    value_fields = []
    row_count = 0
    data_start_row = dataset["header_lines_count"] + 1

    try:
        with file_path.open(
            "r", encoding=dataset["encoding"], newline=""
        ) as handle:
            reader = csv.reader(handle, delimiter=dataset["delimiter"])

            for _ in range(dataset["header_lines_count"]):
                header_lines.append(next(reader))

            value_fields = resolve_value_fields(dataset, header_lines[-1])

            with conn.cursor() as cur:
                if force and existing_hash is not None:
                    cur.execute(
                        f"DELETE FROM {table_name} WHERE record_origin = 'real'"
                    )
                    cur.execute(
                        "DELETE FROM src.dataset_files WHERE dataset_name = %s",
                        (dataset_name,),
                    )

                insert_sql = (
                    f"INSERT INTO {table_name} "
                    "(load_id, row_number, payload, record_origin) VALUES %s"
                )
                batch = []
                row_number = data_start_row

                for row in reader:
                    batch.append((load_id, row_number, Json(row), "real"))
                    row_count += 1
                    row_number += 1
                    if len(batch) >= BATCH_SIZE:
                        execute_values(
                            cur, insert_sql, batch, page_size=BATCH_SIZE
                        )
                        batch.clear()
                        if row_count % 100000 == 0:
                            print(f"  {dataset_name}: {row_count:,} filas")

                if batch:
                    execute_values(cur, insert_sql, batch, page_size=BATCH_SIZE)

                cur.execute(
                    """
                    INSERT INTO src.dataset_files
                    (dataset_name, source_file, encoding, delimiter, header_lines,
                     data_start_row, row_count, sha256, temporal_type, time_field,
                     series_keys, value_fields)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        dataset_name,
                        dataset["file_name"],
                        dataset["encoding"],
                        dataset["delimiter"],
                        Json(header_lines),
                        data_start_row,
                        row_count,
                        file_hash,
                        dataset["temporal_type"],
                        dataset["time_field"],
                        Json(dataset["series_keys"]),
                        Json(value_fields),
                    ),
                )

                finished_at = datetime.now(timezone.utc)
                cur.execute(
                    """
                    INSERT INTO audit.load_log
                    (load_id, dataset_name, source_file, file_hash, rows_loaded,
                     run_type, status, started_at, finished_at)
                    VALUES (%s, %s, %s, %s, %s, 'raw_load', 'success', %s, %s)
                    """,
                    (
                        load_id,
                        dataset_name,
                        dataset["file_name"],
                        file_hash,
                        row_count,
                        started_at,
                        finished_at,
                    ),
                )

        conn.commit()
        print(f"[OK] {dataset_name}: {row_count:,} filas")
    except Exception as exc:
        conn.rollback()
        finished_at = datetime.now(timezone.utc)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO audit.load_log
                    (load_id, dataset_name, source_file, file_hash, rows_loaded,
                     run_type, status, started_at, finished_at, error_message)
                    VALUES (%s, %s, %s, %s, 0, 'raw_load', 'failed', %s, %s, %s)
                    """,
                    (
                        load_id,
                        dataset_name,
                        dataset["file_name"],
                        file_hash,
                        started_at,
                        finished_at,
                        str(exc)[:2000],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        choices=[dataset["dataset_name"] for dataset in DATASETS_CATALOG],
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    selected = [
        dataset
        for dataset in DATASETS_CATALOG
        if args.dataset in (None, dataset["dataset_name"])
    ]

    conn = get_connection()
    try:
        for dataset in selected:
            load_dataset(conn, dataset, force=args.force)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
