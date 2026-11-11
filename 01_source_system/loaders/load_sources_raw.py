"""Carga idempotente de los CSV de origen en las tablas Raw de PostgreSQL.

La capa Raw ya no utiliza columnas ``payload JSONB``. Cada dataset declara en
``DATASETS_CATALOG`` el mapeo explicito entre las columnas fisicas de su tabla
destino y el indice de la columna correspondiente en la fila leida del CSV.

El loader:

* lee cada fichero en modo solo lectura;
* ignora (y conserva en ``src.dataset_files.header_lines``) las filas de
  cabecera indicadas por ``header_rows``;
* desempaqueta cada fila del CSV en columnas fisicas tipadas;
* inserta por lotes con ``execute_values``;
* mantiene la idempotencia mediante el SHA-256 del fichero en
  ``src.dataset_files``;
* registra cada ejecucion en ``audit.load_log``.

Uso:
    python loaders/load_sources_raw.py
    python loaders/load_sources_raw.py --dataset tech_salaries
    python loaders/load_sources_raw.py --force
"""

import argparse
import csv
import hashlib
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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

# Tamano maximo de lote. Se reduce automaticamente en tablas con muchas
# columnas para no superar el limite de parametros del protocolo PostgreSQL.
BATCH_SIZE = 5000
MAX_BIND_PARAMS = 60000

# ---------------------------------------------------------------------------
# Catalogo de datasets
#
# Formato de cada columna: (nombre_fisico, indice_en_la_fila_csv, tipo_sql)
#
#   text     -> TEXT
#   integer  -> INTEGER
#   numeric  -> NUMERIC
#   date     -> DATE
#
# Los indices se refieren a la posicion 0-based dentro de la fila devuelta por
# csv.reader, una vez descartadas las filas de cabecera. Para los datasets de
# formato ancho (ISTAC y TGSS) los nombres de columna se han construido a
# partir de la cabecera real, aplanando periodo + medida cuando la cabecera es
# multinivel.
# ---------------------------------------------------------------------------

DATASETS_CATALOG = [
    {
        "dataset_name": 'cost_of_living',
        "file_name": '(Datos Externos)  costo de vida global -  Kaggle.csv',
        "table_name": 'src.cost_of_living_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'static',
        "time_field": None,
        "series_keys": [],
        "value_fields": ['x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', 'x9', 'x10', 'x11', 'x12', 'x13', 'x14', 'x15', 'x16', 'x17', 'x18', 'x19', 'x20', 'x21', 'x22', 'x23', 'x24', 'x25', 'x26', 'x27', 'x28', 'x29', 'x30', 'x31', 'x32', 'x33', 'x34', 'x35', 'x36', 'x37', 'x38', 'x39', 'x40', 'x41', 'x42', 'x43', 'x44', 'x45', 'x46', 'x47', 'x48', 'x49', 'x50', 'x51', 'x52', 'x53', 'x54', 'x55'],
        "columns": [
            ('city', 0, 'text'),
            ('country', 1, 'text'),
            ('x1', 2, 'numeric'),
            ('x2', 3, 'numeric'),
            ('x3', 4, 'numeric'),
            ('x4', 5, 'numeric'),
            ('x5', 6, 'numeric'),
            ('x6', 7, 'numeric'),
            ('x7', 8, 'numeric'),
            ('x8', 9, 'numeric'),
            ('x9', 10, 'numeric'),
            ('x10', 11, 'numeric'),
            ('x11', 12, 'numeric'),
            ('x12', 13, 'numeric'),
            ('x13', 14, 'numeric'),
            ('x14', 15, 'numeric'),
            ('x15', 16, 'numeric'),
            ('x16', 17, 'numeric'),
            ('x17', 18, 'numeric'),
            ('x18', 19, 'numeric'),
            ('x19', 20, 'numeric'),
            ('x20', 21, 'numeric'),
            ('x21', 22, 'numeric'),
            ('x22', 23, 'numeric'),
            ('x23', 24, 'numeric'),
            ('x24', 25, 'numeric'),
            ('x25', 26, 'numeric'),
            ('x26', 27, 'numeric'),
            ('x27', 28, 'numeric'),
            ('x28', 29, 'numeric'),
            ('x29', 30, 'numeric'),
            ('x30', 31, 'numeric'),
            ('x31', 32, 'numeric'),
            ('x32', 33, 'numeric'),
            ('x33', 34, 'numeric'),
            ('x34', 35, 'numeric'),
            ('x35', 36, 'numeric'),
            ('x36', 37, 'numeric'),
            ('x37', 38, 'numeric'),
            ('x38', 39, 'numeric'),
            ('x39', 40, 'numeric'),
            ('x40', 41, 'numeric'),
            ('x41', 42, 'numeric'),
            ('x42', 43, 'numeric'),
            ('x43', 44, 'numeric'),
            ('x44', 45, 'numeric'),
            ('x45', 46, 'numeric'),
            ('x46', 47, 'numeric'),
            ('x47', 48, 'numeric'),
            ('x48', 49, 'numeric'),
            ('x49', 50, 'numeric'),
            ('x50', 51, 'numeric'),
            ('x51', 52, 'numeric'),
            ('x52', 53, 'numeric'),
            ('x53', 54, 'numeric'),
            ('x54', 55, 'numeric'),
            ('x55', 56, 'numeric'),
            ('data_quality', 57, 'integer'),
        ],
    },
    {
        "dataset_name": 'foreign_affiliates_gender',
        "file_name": '(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social.csv',
        "table_name": 'src.foreign_affiliates_gender_raw',
        "encoding": 'latin-1',
        "delimiter": ';',
        "header_rows": 3,
        "decimal_style": 'dot',
        "temporal_type": 'monthly',
        "time_field": 'periodo',
        "series_keys": ['ccaa'],
        "value_fields": ['total_regimen_total_sexo_total_ue_y_no_ue', 'total_regimen_total_sexo_union_europea', 'total_regimen_total_sexo_no_union', 'total_regimen_mujer_total_ue_y_no_ue', 'total_regimen_mujer_union_europea', 'total_regimen_mujer_no_union', 'total_regimen_varon_total_ue_y_no_ue', 'total_regimen_varon_union_europea', 'total_regimen_varon_no_union', 'total_regimen_no_consta_total_ue_y_no_ue', 'total_regimen_no_consta_union_europea', 'total_regimen_no_consta_no_union', 'regimen_general_total_sexo_total_ue_y_no_ue', 'regimen_general_total_sexo_union_europea', 'regimen_general_total_sexo_no_union', 'regimen_general_mujer_total_ue_y_no_ue', 'regimen_general_mujer_union_europea', 'regimen_general_mujer_no_union', 'regimen_general_varon_total_ue_y_no_ue', 'regimen_general_varon_union_europea', 'regimen_general_varon_no_union', 'regimen_general_no_consta_total_ue_y_no_ue', 'regimen_general_no_consta_union_europea', 'regimen_general_no_consta_no_union', 'sistema_especial_agrario_total_sexo_total_ue_y_no_ue', 'sistema_especial_agrario_total_sexo_union_europea', 'sistema_especial_agrario_total_sexo_no_union', 'sistema_especial_agrario_mujer_total_ue_y_no_ue', 'sistema_especial_agrario_mujer_union_europea', 'sistema_especial_agrario_mujer_no_union', 'sistema_especial_agrario_varon_total_ue_y_no_ue', 'sistema_especial_agrario_varon_union_europea', 'sistema_especial_agrario_varon_no_union', 'sistema_especial_agrario_no_consta_total_ue_y_no_ue', 'sistema_especial_agrario_no_consta_union_europea', 'sistema_especial_agrario_no_consta_no_union', 'sistema_especial_empleados_hogar_total_sexo_total_ue_y_no_ue', 'sistema_especial_empleados_hogar_total_sexo_union_europea', 'sistema_especial_empleados_hogar_total_sexo_no_union', 'sistema_especial_empleados_hogar_mujer_total_ue_y_no_ue', 'sistema_especial_empleados_hogar_mujer_union_europea', 'sistema_especial_empleados_hogar_mujer_no_union', 'sistema_especial_empleados_hogar_varon_total_ue_y_no_ue', 'sistema_especial_empleados_hogar_varon_union_europea', 'sistema_especial_empleados_hogar_varon_no_union', 'sistema_especial_empleados_hogar_no_consta_total_ue_y_no_ue', 'sistema_especial_empleados_hogar_no_consta_union_europea', 'sistema_especial_empleados_hogar_no_consta_no_union', 'regimen_especial_trabajadores_autonomos_no_seta_total_5fe62726', 'regimen_especial_trabajadores_autonomos_no_seta_total_494e85e2', 'regimen_especial_trabajadores_autonomos_no_seta_total_c57b57ba', 'regimen_especial_trabajadores_autonomos_no_seta_mujer_8781c80e', 'regimen_especial_trabajadores_autonomos_no_seta_mujer_9d95b5e7', 'regimen_especial_trabajadores_autonomos_no_seta_mujer_no_union', 'regimen_especial_trabajadores_autonomos_no_seta_varon_6bec5703', 'regimen_especial_trabajadores_autonomos_no_seta_varon_8894d918', 'regimen_especial_trabajadores_autonomos_no_seta_varon_no_union', 'regimen_especial_trabajadores_autonomos_no_seta_no_con_2c20efd9', 'regimen_especial_trabajadores_autonomos_no_seta_no_con_88f93ff9', 'regimen_especial_trabajadores_autonomos_no_seta_no_con_05647fe0', 'regimen_especial_trabajadores_autonomos_seta_total_sex_b946293e', 'regimen_especial_trabajadores_autonomos_seta_total_sex_b4343659', 'regimen_especial_trabajadores_autonomos_seta_total_sex_ddf0fa4b', 'regimen_especial_trabajadores_autonomos_seta_mujer_tot_d2c37c04', 'regimen_especial_trabajadores_autonomos_seta_mujer_uni_877bca87', 'regimen_especial_trabajadores_autonomos_seta_mujer_no_union', 'regimen_especial_trabajadores_autonomos_seta_varon_tot_9dc0c0b7', 'regimen_especial_trabajadores_autonomos_seta_varon_uni_b2b1c2cb', 'regimen_especial_trabajadores_autonomos_seta_varon_no_union', 'regimen_especial_trabajadores_autonomos_seta_no_consta_10b0947f', 'regimen_especial_trabajadores_autonomos_seta_no_consta_a55665ff', 'regimen_especial_trabajadores_autonomos_seta_no_consta_no_union', 'regimen_especial_del_mar_cuenta_ajena_total_sexo_total_cf4483a3', 'regimen_especial_del_mar_cuenta_ajena_total_sexo_union_europea', 'regimen_especial_del_mar_cuenta_ajena_total_sexo_no_union', 'regimen_especial_del_mar_cuenta_ajena_mujer_total_ue_y_no_ue', 'regimen_especial_del_mar_cuenta_ajena_mujer_union_europea', 'regimen_especial_del_mar_cuenta_ajena_mujer_no_union', 'regimen_especial_del_mar_cuenta_ajena_varon_total_ue_y_no_ue', 'regimen_especial_del_mar_cuenta_ajena_varon_union_europea', 'regimen_especial_del_mar_cuenta_ajena_varon_no_union', 'regimen_especial_del_mar_cuenta_ajena_no_consta_total_bc8e8ecb', 'regimen_especial_del_mar_cuenta_ajena_no_consta_union_europea', 'regimen_especial_del_mar_cuenta_ajena_no_consta_no_union', 'regimen_especial_del_mar_cuenta_propia_total_sexo_tota_ab231172', 'regimen_especial_del_mar_cuenta_propia_total_sexo_union_europea', 'regimen_especial_del_mar_cuenta_propia_total_sexo_no_union', 'regimen_especial_del_mar_cuenta_propia_mujer_total_ue_y_no_ue', 'regimen_especial_del_mar_cuenta_propia_mujer_union_europea', 'regimen_especial_del_mar_cuenta_propia_mujer_no_union', 'regimen_especial_del_mar_cuenta_propia_varon_total_ue_y_no_ue', 'regimen_especial_del_mar_cuenta_propia_varon_union_europea', 'regimen_especial_del_mar_cuenta_propia_varon_no_union', 'regimen_especial_del_mar_cuenta_propia_no_consta_total_800e70c2', 'regimen_especial_del_mar_cuenta_propia_no_consta_union_europea', 'regimen_especial_del_mar_cuenta_propia_no_consta_no_union', 'regimen_especial_mineria_carbon_total_sexo_total_ue_y_no_ue', 'regimen_especial_mineria_carbon_total_sexo_union_europea', 'regimen_especial_mineria_carbon_total_sexo_no_union', 'regimen_especial_mineria_carbon_mujer_total_ue_y_no_ue', 'regimen_especial_mineria_carbon_mujer_union_europea', 'regimen_especial_mineria_carbon_mujer_no_union', 'regimen_especial_mineria_carbon_varon_total_ue_y_no_ue', 'regimen_especial_mineria_carbon_varon_union_europea', 'regimen_especial_mineria_carbon_varon_no_union', 'regimen_especial_mineria_carbon_no_consta_total_ue_y_no_ue', 'regimen_especial_mineria_carbon_no_consta_union_europea', 'regimen_especial_mineria_carbon_no_consta_no_union'],
        "columns": [
            ('periodo', 0, 'integer'),
            ('ccaa', 1, 'text'),
            ('total_regimen_total_sexo_total_ue_y_no_ue', 2, 'numeric'),
            ('total_regimen_total_sexo_union_europea', 3, 'numeric'),
            ('total_regimen_total_sexo_no_union', 4, 'numeric'),
            ('total_regimen_mujer_total_ue_y_no_ue', 5, 'numeric'),
            ('total_regimen_mujer_union_europea', 6, 'numeric'),
            ('total_regimen_mujer_no_union', 7, 'numeric'),
            ('total_regimen_varon_total_ue_y_no_ue', 8, 'numeric'),
            ('total_regimen_varon_union_europea', 9, 'numeric'),
            ('total_regimen_varon_no_union', 10, 'numeric'),
            ('total_regimen_no_consta_total_ue_y_no_ue', 11, 'numeric'),
            ('total_regimen_no_consta_union_europea', 12, 'numeric'),
            ('total_regimen_no_consta_no_union', 13, 'numeric'),
            ('regimen_general_total_sexo_total_ue_y_no_ue', 14, 'numeric'),
            ('regimen_general_total_sexo_union_europea', 15, 'numeric'),
            ('regimen_general_total_sexo_no_union', 16, 'numeric'),
            ('regimen_general_mujer_total_ue_y_no_ue', 17, 'numeric'),
            ('regimen_general_mujer_union_europea', 18, 'numeric'),
            ('regimen_general_mujer_no_union', 19, 'numeric'),
            ('regimen_general_varon_total_ue_y_no_ue', 20, 'numeric'),
            ('regimen_general_varon_union_europea', 21, 'numeric'),
            ('regimen_general_varon_no_union', 22, 'numeric'),
            ('regimen_general_no_consta_total_ue_y_no_ue', 23, 'numeric'),
            ('regimen_general_no_consta_union_europea', 24, 'numeric'),
            ('regimen_general_no_consta_no_union', 25, 'numeric'),
            ('sistema_especial_agrario_total_sexo_total_ue_y_no_ue', 26, 'numeric'),
            ('sistema_especial_agrario_total_sexo_union_europea', 27, 'numeric'),
            ('sistema_especial_agrario_total_sexo_no_union', 28, 'numeric'),
            ('sistema_especial_agrario_mujer_total_ue_y_no_ue', 29, 'numeric'),
            ('sistema_especial_agrario_mujer_union_europea', 30, 'numeric'),
            ('sistema_especial_agrario_mujer_no_union', 31, 'numeric'),
            ('sistema_especial_agrario_varon_total_ue_y_no_ue', 32, 'numeric'),
            ('sistema_especial_agrario_varon_union_europea', 33, 'numeric'),
            ('sistema_especial_agrario_varon_no_union', 34, 'numeric'),
            ('sistema_especial_agrario_no_consta_total_ue_y_no_ue', 35, 'numeric'),
            ('sistema_especial_agrario_no_consta_union_europea', 36, 'numeric'),
            ('sistema_especial_agrario_no_consta_no_union', 37, 'numeric'),
            ('sistema_especial_empleados_hogar_total_sexo_total_ue_y_no_ue', 38, 'numeric'),
            ('sistema_especial_empleados_hogar_total_sexo_union_europea', 39, 'numeric'),
            ('sistema_especial_empleados_hogar_total_sexo_no_union', 40, 'numeric'),
            ('sistema_especial_empleados_hogar_mujer_total_ue_y_no_ue', 41, 'numeric'),
            ('sistema_especial_empleados_hogar_mujer_union_europea', 42, 'numeric'),
            ('sistema_especial_empleados_hogar_mujer_no_union', 43, 'numeric'),
            ('sistema_especial_empleados_hogar_varon_total_ue_y_no_ue', 44, 'numeric'),
            ('sistema_especial_empleados_hogar_varon_union_europea', 45, 'numeric'),
            ('sistema_especial_empleados_hogar_varon_no_union', 46, 'numeric'),
            ('sistema_especial_empleados_hogar_no_consta_total_ue_y_no_ue', 47, 'numeric'),
            ('sistema_especial_empleados_hogar_no_consta_union_europea', 48, 'numeric'),
            ('sistema_especial_empleados_hogar_no_consta_no_union', 49, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_total_5fe62726', 50, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_total_494e85e2', 51, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_total_c57b57ba', 52, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_mujer_8781c80e', 53, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_mujer_9d95b5e7', 54, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_mujer_no_union', 55, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_varon_6bec5703', 56, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_varon_8894d918', 57, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_varon_no_union', 58, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_no_con_2c20efd9', 59, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_no_con_88f93ff9', 60, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_no_seta_no_con_05647fe0', 61, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_total_sex_b946293e', 62, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_total_sex_b4343659', 63, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_total_sex_ddf0fa4b', 64, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_mujer_tot_d2c37c04', 65, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_mujer_uni_877bca87', 66, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_mujer_no_union', 67, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_varon_tot_9dc0c0b7', 68, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_varon_uni_b2b1c2cb', 69, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_varon_no_union', 70, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_no_consta_10b0947f', 71, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_no_consta_a55665ff', 72, 'numeric'),
            ('regimen_especial_trabajadores_autonomos_seta_no_consta_no_union', 73, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_total_sexo_total_cf4483a3', 74, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_total_sexo_union_europea', 75, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_total_sexo_no_union', 76, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_mujer_total_ue_y_no_ue', 77, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_mujer_union_europea', 78, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_mujer_no_union', 79, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_varon_total_ue_y_no_ue', 80, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_varon_union_europea', 81, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_varon_no_union', 82, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_no_consta_total_bc8e8ecb', 83, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_no_consta_union_europea', 84, 'numeric'),
            ('regimen_especial_del_mar_cuenta_ajena_no_consta_no_union', 85, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_total_sexo_tota_ab231172', 86, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_total_sexo_union_europea', 87, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_total_sexo_no_union', 88, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_mujer_total_ue_y_no_ue', 89, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_mujer_union_europea', 90, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_mujer_no_union', 91, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_varon_total_ue_y_no_ue', 92, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_varon_union_europea', 93, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_varon_no_union', 94, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_no_consta_total_800e70c2', 95, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_no_consta_union_europea', 96, 'numeric'),
            ('regimen_especial_del_mar_cuenta_propia_no_consta_no_union', 97, 'numeric'),
            ('regimen_especial_mineria_carbon_total_sexo_total_ue_y_no_ue', 98, 'numeric'),
            ('regimen_especial_mineria_carbon_total_sexo_union_europea', 99, 'numeric'),
            ('regimen_especial_mineria_carbon_total_sexo_no_union', 100, 'numeric'),
            ('regimen_especial_mineria_carbon_mujer_total_ue_y_no_ue', 101, 'numeric'),
            ('regimen_especial_mineria_carbon_mujer_union_europea', 102, 'numeric'),
            ('regimen_especial_mineria_carbon_mujer_no_union', 103, 'numeric'),
            ('regimen_especial_mineria_carbon_varon_total_ue_y_no_ue', 104, 'numeric'),
            ('regimen_especial_mineria_carbon_varon_union_europea', 105, 'numeric'),
            ('regimen_especial_mineria_carbon_varon_no_union', 106, 'numeric'),
            ('regimen_especial_mineria_carbon_no_consta_total_ue_y_no_ue', 107, 'numeric'),
            ('regimen_especial_mineria_carbon_no_consta_union_europea', 108, 'numeric'),
            ('regimen_especial_mineria_carbon_no_consta_no_union', 109, 'numeric'),
        ],
    },
    {
        "dataset_name": 'eurostat_hitech_employment',
        "file_name": '(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv',
        "table_name": 'src.eurostat_hitech_employment_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'annual',
        "time_field": 'time_period',
        "series_keys": ['freq', 'category', 'isco08', 'age', 'unit', 'geo'],
        "value_fields": ['obs_value'],
        "columns": [
            ('structure', 0, 'text'),
            ('structure_id', 1, 'text'),
            ('structure_name', 2, 'text'),
            ('freq', 3, 'text'),
            ('freq_label', 4, 'text'),
            ('category', 5, 'text'),
            ('category_label', 6, 'text'),
            ('isco08', 7, 'text'),
            ('isco08_label', 8, 'text'),
            ('age', 9, 'text'),
            ('age_label', 10, 'text'),
            ('unit', 11, 'text'),
            ('unit_label', 12, 'text'),
            ('geo', 13, 'text'),
            ('geo_label', 14, 'text'),
            ('time_period', 15, 'integer'),
            ('time_label', 16, 'text'),
            ('obs_value', 17, 'numeric'),
            ('obs_value_label', 18, 'text'),
            ('obs_flag', 19, 'text'),
            ('obs_flag_label', 20, 'text'),
            ('conf_status', 21, 'text'),
            ('conf_status_label', 22, 'text'),
        ],
    },
    {
        "dataset_name": 'epa_labor_market',
        "file_name": '(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv',
        "table_name": 'src.epa_labor_market_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 2,
        "decimal_style": 'comma',
        "temporal_type": 'static',
        "time_field": None,
        "series_keys": [],
        "value_fields": ['hombres_ocupados', 'hombres_parados', 'hombres_tasa_actividad', 'hombres_tasa_paro', 'mujeres_ocupadas', 'mujeres_paradas', 'mujeres_tasa_actividad', 'mujeres_tasa_paro'],
        "columns": [
            ('region', 0, 'text'),
            ('hombres_ocupados', 2, 'numeric'),
            ('hombres_parados', 4, 'numeric'),
            ('hombres_tasa_actividad', 6, 'numeric'),
            ('hombres_tasa_paro', 8, 'numeric'),
            ('mujeres_ocupadas', 10, 'numeric'),
            ('mujeres_paradas', 12, 'numeric'),
            ('mujeres_tasa_actividad', 14, 'numeric'),
            ('mujeres_tasa_paro', 16, 'numeric'),
        ],
    },
    {
        "dataset_name": 'istac_employment_cnae',
        "file_name": '(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv',
        "table_name": 'src.istac_employment_cnae_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 2,
        "decimal_style": 'dot',
        "temporal_type": 'quarterly',
        "time_field": None,
        "series_keys": ['geography', 'activity'],
        "value_fields": ['total_2025', 'empleos_por_cuenta_ajena_2025', 'empleos_por_cuenta_propia_2025', 'total_2025_cuarto_trimestre', 'empleos_por_cuenta_ajena_2025_cuarto_trimestre', 'empleos_por_cuenta_propia_2025_cuarto_trimestre', 'total_2025_tercer_trimestre', 'empleos_por_cuenta_ajena_2025_tercer_trimestre', 'empleos_por_cuenta_propia_2025_tercer_trimestre', 'total_2025_segundo_trimestre', 'empleos_por_cuenta_ajena_2025_segundo_trimestre', 'empleos_por_cuenta_propia_2025_segundo_trimestre', 'total_2025_primer_trimestre', 'empleos_por_cuenta_ajena_2025_primer_trimestre', 'empleos_por_cuenta_propia_2025_primer_trimestre', 'total_2024', 'empleos_por_cuenta_ajena_2024', 'empleos_por_cuenta_propia_2024', 'total_2024_cuarto_trimestre', 'empleos_por_cuenta_ajena_2024_cuarto_trimestre', 'empleos_por_cuenta_propia_2024_cuarto_trimestre', 'total_2024_tercer_trimestre', 'empleos_por_cuenta_ajena_2024_tercer_trimestre', 'empleos_por_cuenta_propia_2024_tercer_trimestre', 'total_2024_segundo_trimestre', 'empleos_por_cuenta_ajena_2024_segundo_trimestre', 'empleos_por_cuenta_propia_2024_segundo_trimestre', 'total_2024_primer_trimestre', 'empleos_por_cuenta_ajena_2024_primer_trimestre', 'empleos_por_cuenta_propia_2024_primer_trimestre', 'total_2023', 'empleos_por_cuenta_ajena_2023', 'empleos_por_cuenta_propia_2023', 'total_2023_cuarto_trimestre', 'empleos_por_cuenta_ajena_2023_cuarto_trimestre', 'empleos_por_cuenta_propia_2023_cuarto_trimestre', 'total_2023_tercer_trimestre', 'empleos_por_cuenta_ajena_2023_tercer_trimestre', 'empleos_por_cuenta_propia_2023_tercer_trimestre', 'total_2023_segundo_trimestre', 'empleos_por_cuenta_ajena_2023_segundo_trimestre', 'empleos_por_cuenta_propia_2023_segundo_trimestre', 'total_2023_primer_trimestre', 'empleos_por_cuenta_ajena_2023_primer_trimestre', 'empleos_por_cuenta_propia_2023_primer_trimestre', 'total_2022', 'empleos_por_cuenta_ajena_2022', 'empleos_por_cuenta_propia_2022', 'total_2022_cuarto_trimestre', 'empleos_por_cuenta_ajena_2022_cuarto_trimestre', 'empleos_por_cuenta_propia_2022_cuarto_trimestre', 'total_2022_tercer_trimestre', 'empleos_por_cuenta_ajena_2022_tercer_trimestre', 'empleos_por_cuenta_propia_2022_tercer_trimestre', 'total_2022_segundo_trimestre', 'empleos_por_cuenta_ajena_2022_segundo_trimestre', 'empleos_por_cuenta_propia_2022_segundo_trimestre', 'total_2022_primer_trimestre', 'empleos_por_cuenta_ajena_2022_primer_trimestre', 'empleos_por_cuenta_propia_2022_primer_trimestre', 'total_2021', 'empleos_por_cuenta_ajena_2021', 'empleos_por_cuenta_propia_2021', 'total_2021_cuarto_trimestre', 'empleos_por_cuenta_ajena_2021_cuarto_trimestre', 'empleos_por_cuenta_propia_2021_cuarto_trimestre', 'total_2021_tercer_trimestre', 'empleos_por_cuenta_ajena_2021_tercer_trimestre', 'empleos_por_cuenta_propia_2021_tercer_trimestre', 'total_2021_segundo_trimestre', 'empleos_por_cuenta_ajena_2021_segundo_trimestre', 'empleos_por_cuenta_propia_2021_segundo_trimestre', 'total_2021_primer_trimestre', 'empleos_por_cuenta_ajena_2021_primer_trimestre', 'empleos_por_cuenta_propia_2021_primer_trimestre', 'total_2020', 'empleos_por_cuenta_ajena_2020', 'empleos_por_cuenta_propia_2020', 'total_2020_cuarto_trimestre', 'empleos_por_cuenta_ajena_2020_cuarto_trimestre', 'empleos_por_cuenta_propia_2020_cuarto_trimestre', 'total_2020_tercer_trimestre', 'empleos_por_cuenta_ajena_2020_tercer_trimestre', 'empleos_por_cuenta_propia_2020_tercer_trimestre', 'total_2020_segundo_trimestre', 'empleos_por_cuenta_ajena_2020_segundo_trimestre', 'empleos_por_cuenta_propia_2020_segundo_trimestre', 'total_2020_primer_trimestre', 'empleos_por_cuenta_ajena_2020_primer_trimestre', 'empleos_por_cuenta_propia_2020_primer_trimestre', 'total_2019', 'empleos_por_cuenta_ajena_2019', 'empleos_por_cuenta_propia_2019', 'total_2019_cuarto_trimestre', 'empleos_por_cuenta_ajena_2019_cuarto_trimestre', 'empleos_por_cuenta_propia_2019_cuarto_trimestre', 'total_2019_tercer_trimestre', 'empleos_por_cuenta_ajena_2019_tercer_trimestre', 'empleos_por_cuenta_propia_2019_tercer_trimestre', 'total_2019_segundo_trimestre', 'empleos_por_cuenta_ajena_2019_segundo_trimestre', 'empleos_por_cuenta_propia_2019_segundo_trimestre', 'total_2019_primer_trimestre', 'empleos_por_cuenta_ajena_2019_primer_trimestre', 'empleos_por_cuenta_propia_2019_primer_trimestre', 'total_2018', 'empleos_por_cuenta_ajena_2018', 'empleos_por_cuenta_propia_2018', 'total_2018_cuarto_trimestre', 'empleos_por_cuenta_ajena_2018_cuarto_trimestre', 'empleos_por_cuenta_propia_2018_cuarto_trimestre', 'total_2018_tercer_trimestre', 'empleos_por_cuenta_ajena_2018_tercer_trimestre', 'empleos_por_cuenta_propia_2018_tercer_trimestre', 'total_2018_segundo_trimestre', 'empleos_por_cuenta_ajena_2018_segundo_trimestre', 'empleos_por_cuenta_propia_2018_segundo_trimestre', 'total_2018_primer_trimestre', 'empleos_por_cuenta_ajena_2018_primer_trimestre', 'empleos_por_cuenta_propia_2018_primer_trimestre', 'total_2017', 'empleos_por_cuenta_ajena_2017', 'empleos_por_cuenta_propia_2017', 'total_2017_cuarto_trimestre', 'empleos_por_cuenta_ajena_2017_cuarto_trimestre', 'empleos_por_cuenta_propia_2017_cuarto_trimestre', 'total_2017_tercer_trimestre', 'empleos_por_cuenta_ajena_2017_tercer_trimestre', 'empleos_por_cuenta_propia_2017_tercer_trimestre', 'total_2017_segundo_trimestre', 'empleos_por_cuenta_ajena_2017_segundo_trimestre', 'empleos_por_cuenta_propia_2017_segundo_trimestre', 'total_2017_primer_trimestre', 'empleos_por_cuenta_ajena_2017_primer_trimestre', 'empleos_por_cuenta_propia_2017_primer_trimestre', 'total_2016', 'empleos_por_cuenta_ajena_2016', 'empleos_por_cuenta_propia_2016', 'total_2016_cuarto_trimestre', 'empleos_por_cuenta_ajena_2016_cuarto_trimestre', 'empleos_por_cuenta_propia_2016_cuarto_trimestre', 'total_2016_tercer_trimestre', 'empleos_por_cuenta_ajena_2016_tercer_trimestre', 'empleos_por_cuenta_propia_2016_tercer_trimestre', 'total_2016_segundo_trimestre', 'empleos_por_cuenta_ajena_2016_segundo_trimestre', 'empleos_por_cuenta_propia_2016_segundo_trimestre', 'total_2016_primer_trimestre', 'empleos_por_cuenta_ajena_2016_primer_trimestre', 'empleos_por_cuenta_propia_2016_primer_trimestre', 'total_2015', 'empleos_por_cuenta_ajena_2015', 'empleos_por_cuenta_propia_2015', 'total_2015_cuarto_trimestre', 'empleos_por_cuenta_ajena_2015_cuarto_trimestre', 'empleos_por_cuenta_propia_2015_cuarto_trimestre', 'total_2015_tercer_trimestre', 'empleos_por_cuenta_ajena_2015_tercer_trimestre', 'empleos_por_cuenta_propia_2015_tercer_trimestre', 'total_2015_segundo_trimestre', 'empleos_por_cuenta_ajena_2015_segundo_trimestre', 'empleos_por_cuenta_propia_2015_segundo_trimestre', 'total_2015_primer_trimestre', 'empleos_por_cuenta_ajena_2015_primer_trimestre', 'empleos_por_cuenta_propia_2015_primer_trimestre', 'total_2014', 'empleos_por_cuenta_ajena_2014', 'empleos_por_cuenta_propia_2014', 'total_2014_cuarto_trimestre', 'empleos_por_cuenta_ajena_2014_cuarto_trimestre', 'empleos_por_cuenta_propia_2014_cuarto_trimestre', 'total_2014_tercer_trimestre', 'empleos_por_cuenta_ajena_2014_tercer_trimestre', 'empleos_por_cuenta_propia_2014_tercer_trimestre', 'total_2014_segundo_trimestre', 'empleos_por_cuenta_ajena_2014_segundo_trimestre', 'empleos_por_cuenta_propia_2014_segundo_trimestre', 'total_2014_primer_trimestre', 'empleos_por_cuenta_ajena_2014_primer_trimestre', 'empleos_por_cuenta_propia_2014_primer_trimestre', 'total_2013', 'empleos_por_cuenta_ajena_2013', 'empleos_por_cuenta_propia_2013', 'total_2013_cuarto_trimestre', 'empleos_por_cuenta_ajena_2013_cuarto_trimestre', 'empleos_por_cuenta_propia_2013_cuarto_trimestre', 'total_2013_tercer_trimestre', 'empleos_por_cuenta_ajena_2013_tercer_trimestre', 'empleos_por_cuenta_propia_2013_tercer_trimestre', 'total_2013_segundo_trimestre', 'empleos_por_cuenta_ajena_2013_segundo_trimestre', 'empleos_por_cuenta_propia_2013_segundo_trimestre', 'total_2013_primer_trimestre', 'empleos_por_cuenta_ajena_2013_primer_trimestre', 'empleos_por_cuenta_propia_2013_primer_trimestre', 'total_2012', 'empleos_por_cuenta_ajena_2012', 'empleos_por_cuenta_propia_2012', 'total_2012_cuarto_trimestre', 'empleos_por_cuenta_ajena_2012_cuarto_trimestre', 'empleos_por_cuenta_propia_2012_cuarto_trimestre', 'total_2012_tercer_trimestre', 'empleos_por_cuenta_ajena_2012_tercer_trimestre', 'empleos_por_cuenta_propia_2012_tercer_trimestre', 'total_2012_segundo_trimestre', 'empleos_por_cuenta_ajena_2012_segundo_trimestre', 'empleos_por_cuenta_propia_2012_segundo_trimestre', 'total_2012_primer_trimestre', 'empleos_por_cuenta_ajena_2012_primer_trimestre', 'empleos_por_cuenta_propia_2012_primer_trimestre', 'total_2011', 'empleos_por_cuenta_ajena_2011', 'empleos_por_cuenta_propia_2011', 'total_2011_cuarto_trimestre', 'empleos_por_cuenta_ajena_2011_cuarto_trimestre', 'empleos_por_cuenta_propia_2011_cuarto_trimestre', 'total_2011_tercer_trimestre', 'empleos_por_cuenta_ajena_2011_tercer_trimestre', 'empleos_por_cuenta_propia_2011_tercer_trimestre', 'total_2011_segundo_trimestre', 'empleos_por_cuenta_ajena_2011_segundo_trimestre', 'empleos_por_cuenta_propia_2011_segundo_trimestre', 'total_2011_primer_trimestre', 'empleos_por_cuenta_ajena_2011_primer_trimestre', 'empleos_por_cuenta_propia_2011_primer_trimestre', 'total_2010', 'empleos_por_cuenta_ajena_2010', 'empleos_por_cuenta_propia_2010', 'total_2010_cuarto_trimestre', 'empleos_por_cuenta_ajena_2010_cuarto_trimestre', 'empleos_por_cuenta_propia_2010_cuarto_trimestre', 'total_2010_tercer_trimestre', 'empleos_por_cuenta_ajena_2010_tercer_trimestre', 'empleos_por_cuenta_propia_2010_tercer_trimestre', 'total_2010_segundo_trimestre', 'empleos_por_cuenta_ajena_2010_segundo_trimestre', 'empleos_por_cuenta_propia_2010_segundo_trimestre', 'total_2010_primer_trimestre', 'empleos_por_cuenta_ajena_2010_primer_trimestre', 'empleos_por_cuenta_propia_2010_primer_trimestre', 'total_2009', 'empleos_por_cuenta_ajena_2009', 'empleos_por_cuenta_propia_2009', 'total_2009_cuarto_trimestre', 'empleos_por_cuenta_ajena_2009_cuarto_trimestre', 'empleos_por_cuenta_propia_2009_cuarto_trimestre', 'total_2009_tercer_trimestre', 'empleos_por_cuenta_ajena_2009_tercer_trimestre', 'empleos_por_cuenta_propia_2009_tercer_trimestre', 'total_2009_segundo_trimestre', 'empleos_por_cuenta_ajena_2009_segundo_trimestre', 'empleos_por_cuenta_propia_2009_segundo_trimestre', 'total_2009_primer_trimestre', 'empleos_por_cuenta_ajena_2009_primer_trimestre', 'empleos_por_cuenta_propia_2009_primer_trimestre'],
        "columns": [
            ('geography', 0, 'text'),
            ('activity', 1, 'text'),
            ('total_2025', 2, 'numeric'),
            ('empleos_por_cuenta_ajena_2025', 3, 'numeric'),
            ('empleos_por_cuenta_propia_2025', 4, 'numeric'),
            ('total_2025_cuarto_trimestre', 5, 'numeric'),
            ('empleos_por_cuenta_ajena_2025_cuarto_trimestre', 6, 'numeric'),
            ('empleos_por_cuenta_propia_2025_cuarto_trimestre', 7, 'numeric'),
            ('total_2025_tercer_trimestre', 8, 'numeric'),
            ('empleos_por_cuenta_ajena_2025_tercer_trimestre', 9, 'numeric'),
            ('empleos_por_cuenta_propia_2025_tercer_trimestre', 10, 'numeric'),
            ('total_2025_segundo_trimestre', 11, 'numeric'),
            ('empleos_por_cuenta_ajena_2025_segundo_trimestre', 12, 'numeric'),
            ('empleos_por_cuenta_propia_2025_segundo_trimestre', 13, 'numeric'),
            ('total_2025_primer_trimestre', 14, 'numeric'),
            ('empleos_por_cuenta_ajena_2025_primer_trimestre', 15, 'numeric'),
            ('empleos_por_cuenta_propia_2025_primer_trimestre', 16, 'numeric'),
            ('total_2024', 17, 'numeric'),
            ('empleos_por_cuenta_ajena_2024', 18, 'numeric'),
            ('empleos_por_cuenta_propia_2024', 19, 'numeric'),
            ('total_2024_cuarto_trimestre', 20, 'numeric'),
            ('empleos_por_cuenta_ajena_2024_cuarto_trimestre', 21, 'numeric'),
            ('empleos_por_cuenta_propia_2024_cuarto_trimestre', 22, 'numeric'),
            ('total_2024_tercer_trimestre', 23, 'numeric'),
            ('empleos_por_cuenta_ajena_2024_tercer_trimestre', 24, 'numeric'),
            ('empleos_por_cuenta_propia_2024_tercer_trimestre', 25, 'numeric'),
            ('total_2024_segundo_trimestre', 26, 'numeric'),
            ('empleos_por_cuenta_ajena_2024_segundo_trimestre', 27, 'numeric'),
            ('empleos_por_cuenta_propia_2024_segundo_trimestre', 28, 'numeric'),
            ('total_2024_primer_trimestre', 29, 'numeric'),
            ('empleos_por_cuenta_ajena_2024_primer_trimestre', 30, 'numeric'),
            ('empleos_por_cuenta_propia_2024_primer_trimestre', 31, 'numeric'),
            ('total_2023', 32, 'numeric'),
            ('empleos_por_cuenta_ajena_2023', 33, 'numeric'),
            ('empleos_por_cuenta_propia_2023', 34, 'numeric'),
            ('total_2023_cuarto_trimestre', 35, 'numeric'),
            ('empleos_por_cuenta_ajena_2023_cuarto_trimestre', 36, 'numeric'),
            ('empleos_por_cuenta_propia_2023_cuarto_trimestre', 37, 'numeric'),
            ('total_2023_tercer_trimestre', 38, 'numeric'),
            ('empleos_por_cuenta_ajena_2023_tercer_trimestre', 39, 'numeric'),
            ('empleos_por_cuenta_propia_2023_tercer_trimestre', 40, 'numeric'),
            ('total_2023_segundo_trimestre', 41, 'numeric'),
            ('empleos_por_cuenta_ajena_2023_segundo_trimestre', 42, 'numeric'),
            ('empleos_por_cuenta_propia_2023_segundo_trimestre', 43, 'numeric'),
            ('total_2023_primer_trimestre', 44, 'numeric'),
            ('empleos_por_cuenta_ajena_2023_primer_trimestre', 45, 'numeric'),
            ('empleos_por_cuenta_propia_2023_primer_trimestre', 46, 'numeric'),
            ('total_2022', 47, 'numeric'),
            ('empleos_por_cuenta_ajena_2022', 48, 'numeric'),
            ('empleos_por_cuenta_propia_2022', 49, 'numeric'),
            ('total_2022_cuarto_trimestre', 50, 'numeric'),
            ('empleos_por_cuenta_ajena_2022_cuarto_trimestre', 51, 'numeric'),
            ('empleos_por_cuenta_propia_2022_cuarto_trimestre', 52, 'numeric'),
            ('total_2022_tercer_trimestre', 53, 'numeric'),
            ('empleos_por_cuenta_ajena_2022_tercer_trimestre', 54, 'numeric'),
            ('empleos_por_cuenta_propia_2022_tercer_trimestre', 55, 'numeric'),
            ('total_2022_segundo_trimestre', 56, 'numeric'),
            ('empleos_por_cuenta_ajena_2022_segundo_trimestre', 57, 'numeric'),
            ('empleos_por_cuenta_propia_2022_segundo_trimestre', 58, 'numeric'),
            ('total_2022_primer_trimestre', 59, 'numeric'),
            ('empleos_por_cuenta_ajena_2022_primer_trimestre', 60, 'numeric'),
            ('empleos_por_cuenta_propia_2022_primer_trimestre', 61, 'numeric'),
            ('total_2021', 62, 'numeric'),
            ('empleos_por_cuenta_ajena_2021', 63, 'numeric'),
            ('empleos_por_cuenta_propia_2021', 64, 'numeric'),
            ('total_2021_cuarto_trimestre', 65, 'numeric'),
            ('empleos_por_cuenta_ajena_2021_cuarto_trimestre', 66, 'numeric'),
            ('empleos_por_cuenta_propia_2021_cuarto_trimestre', 67, 'numeric'),
            ('total_2021_tercer_trimestre', 68, 'numeric'),
            ('empleos_por_cuenta_ajena_2021_tercer_trimestre', 69, 'numeric'),
            ('empleos_por_cuenta_propia_2021_tercer_trimestre', 70, 'numeric'),
            ('total_2021_segundo_trimestre', 71, 'numeric'),
            ('empleos_por_cuenta_ajena_2021_segundo_trimestre', 72, 'numeric'),
            ('empleos_por_cuenta_propia_2021_segundo_trimestre', 73, 'numeric'),
            ('total_2021_primer_trimestre', 74, 'numeric'),
            ('empleos_por_cuenta_ajena_2021_primer_trimestre', 75, 'numeric'),
            ('empleos_por_cuenta_propia_2021_primer_trimestre', 76, 'numeric'),
            ('total_2020', 77, 'numeric'),
            ('empleos_por_cuenta_ajena_2020', 78, 'numeric'),
            ('empleos_por_cuenta_propia_2020', 79, 'numeric'),
            ('total_2020_cuarto_trimestre', 80, 'numeric'),
            ('empleos_por_cuenta_ajena_2020_cuarto_trimestre', 81, 'numeric'),
            ('empleos_por_cuenta_propia_2020_cuarto_trimestre', 82, 'numeric'),
            ('total_2020_tercer_trimestre', 83, 'numeric'),
            ('empleos_por_cuenta_ajena_2020_tercer_trimestre', 84, 'numeric'),
            ('empleos_por_cuenta_propia_2020_tercer_trimestre', 85, 'numeric'),
            ('total_2020_segundo_trimestre', 86, 'numeric'),
            ('empleos_por_cuenta_ajena_2020_segundo_trimestre', 87, 'numeric'),
            ('empleos_por_cuenta_propia_2020_segundo_trimestre', 88, 'numeric'),
            ('total_2020_primer_trimestre', 89, 'numeric'),
            ('empleos_por_cuenta_ajena_2020_primer_trimestre', 90, 'numeric'),
            ('empleos_por_cuenta_propia_2020_primer_trimestre', 91, 'numeric'),
            ('total_2019', 92, 'numeric'),
            ('empleos_por_cuenta_ajena_2019', 93, 'numeric'),
            ('empleos_por_cuenta_propia_2019', 94, 'numeric'),
            ('total_2019_cuarto_trimestre', 95, 'numeric'),
            ('empleos_por_cuenta_ajena_2019_cuarto_trimestre', 96, 'numeric'),
            ('empleos_por_cuenta_propia_2019_cuarto_trimestre', 97, 'numeric'),
            ('total_2019_tercer_trimestre', 98, 'numeric'),
            ('empleos_por_cuenta_ajena_2019_tercer_trimestre', 99, 'numeric'),
            ('empleos_por_cuenta_propia_2019_tercer_trimestre', 100, 'numeric'),
            ('total_2019_segundo_trimestre', 101, 'numeric'),
            ('empleos_por_cuenta_ajena_2019_segundo_trimestre', 102, 'numeric'),
            ('empleos_por_cuenta_propia_2019_segundo_trimestre', 103, 'numeric'),
            ('total_2019_primer_trimestre', 104, 'numeric'),
            ('empleos_por_cuenta_ajena_2019_primer_trimestre', 105, 'numeric'),
            ('empleos_por_cuenta_propia_2019_primer_trimestre', 106, 'numeric'),
            ('total_2018', 107, 'numeric'),
            ('empleos_por_cuenta_ajena_2018', 108, 'numeric'),
            ('empleos_por_cuenta_propia_2018', 109, 'numeric'),
            ('total_2018_cuarto_trimestre', 110, 'numeric'),
            ('empleos_por_cuenta_ajena_2018_cuarto_trimestre', 111, 'numeric'),
            ('empleos_por_cuenta_propia_2018_cuarto_trimestre', 112, 'numeric'),
            ('total_2018_tercer_trimestre', 113, 'numeric'),
            ('empleos_por_cuenta_ajena_2018_tercer_trimestre', 114, 'numeric'),
            ('empleos_por_cuenta_propia_2018_tercer_trimestre', 115, 'numeric'),
            ('total_2018_segundo_trimestre', 116, 'numeric'),
            ('empleos_por_cuenta_ajena_2018_segundo_trimestre', 117, 'numeric'),
            ('empleos_por_cuenta_propia_2018_segundo_trimestre', 118, 'numeric'),
            ('total_2018_primer_trimestre', 119, 'numeric'),
            ('empleos_por_cuenta_ajena_2018_primer_trimestre', 120, 'numeric'),
            ('empleos_por_cuenta_propia_2018_primer_trimestre', 121, 'numeric'),
            ('total_2017', 122, 'numeric'),
            ('empleos_por_cuenta_ajena_2017', 123, 'numeric'),
            ('empleos_por_cuenta_propia_2017', 124, 'numeric'),
            ('total_2017_cuarto_trimestre', 125, 'numeric'),
            ('empleos_por_cuenta_ajena_2017_cuarto_trimestre', 126, 'numeric'),
            ('empleos_por_cuenta_propia_2017_cuarto_trimestre', 127, 'numeric'),
            ('total_2017_tercer_trimestre', 128, 'numeric'),
            ('empleos_por_cuenta_ajena_2017_tercer_trimestre', 129, 'numeric'),
            ('empleos_por_cuenta_propia_2017_tercer_trimestre', 130, 'numeric'),
            ('total_2017_segundo_trimestre', 131, 'numeric'),
            ('empleos_por_cuenta_ajena_2017_segundo_trimestre', 132, 'numeric'),
            ('empleos_por_cuenta_propia_2017_segundo_trimestre', 133, 'numeric'),
            ('total_2017_primer_trimestre', 134, 'numeric'),
            ('empleos_por_cuenta_ajena_2017_primer_trimestre', 135, 'numeric'),
            ('empleos_por_cuenta_propia_2017_primer_trimestre', 136, 'numeric'),
            ('total_2016', 137, 'numeric'),
            ('empleos_por_cuenta_ajena_2016', 138, 'numeric'),
            ('empleos_por_cuenta_propia_2016', 139, 'numeric'),
            ('total_2016_cuarto_trimestre', 140, 'numeric'),
            ('empleos_por_cuenta_ajena_2016_cuarto_trimestre', 141, 'numeric'),
            ('empleos_por_cuenta_propia_2016_cuarto_trimestre', 142, 'numeric'),
            ('total_2016_tercer_trimestre', 143, 'numeric'),
            ('empleos_por_cuenta_ajena_2016_tercer_trimestre', 144, 'numeric'),
            ('empleos_por_cuenta_propia_2016_tercer_trimestre', 145, 'numeric'),
            ('total_2016_segundo_trimestre', 146, 'numeric'),
            ('empleos_por_cuenta_ajena_2016_segundo_trimestre', 147, 'numeric'),
            ('empleos_por_cuenta_propia_2016_segundo_trimestre', 148, 'numeric'),
            ('total_2016_primer_trimestre', 149, 'numeric'),
            ('empleos_por_cuenta_ajena_2016_primer_trimestre', 150, 'numeric'),
            ('empleos_por_cuenta_propia_2016_primer_trimestre', 151, 'numeric'),
            ('total_2015', 152, 'numeric'),
            ('empleos_por_cuenta_ajena_2015', 153, 'numeric'),
            ('empleos_por_cuenta_propia_2015', 154, 'numeric'),
            ('total_2015_cuarto_trimestre', 155, 'numeric'),
            ('empleos_por_cuenta_ajena_2015_cuarto_trimestre', 156, 'numeric'),
            ('empleos_por_cuenta_propia_2015_cuarto_trimestre', 157, 'numeric'),
            ('total_2015_tercer_trimestre', 158, 'numeric'),
            ('empleos_por_cuenta_ajena_2015_tercer_trimestre', 159, 'numeric'),
            ('empleos_por_cuenta_propia_2015_tercer_trimestre', 160, 'numeric'),
            ('total_2015_segundo_trimestre', 161, 'numeric'),
            ('empleos_por_cuenta_ajena_2015_segundo_trimestre', 162, 'numeric'),
            ('empleos_por_cuenta_propia_2015_segundo_trimestre', 163, 'numeric'),
            ('total_2015_primer_trimestre', 164, 'numeric'),
            ('empleos_por_cuenta_ajena_2015_primer_trimestre', 165, 'numeric'),
            ('empleos_por_cuenta_propia_2015_primer_trimestre', 166, 'numeric'),
            ('total_2014', 167, 'numeric'),
            ('empleos_por_cuenta_ajena_2014', 168, 'numeric'),
            ('empleos_por_cuenta_propia_2014', 169, 'numeric'),
            ('total_2014_cuarto_trimestre', 170, 'numeric'),
            ('empleos_por_cuenta_ajena_2014_cuarto_trimestre', 171, 'numeric'),
            ('empleos_por_cuenta_propia_2014_cuarto_trimestre', 172, 'numeric'),
            ('total_2014_tercer_trimestre', 173, 'numeric'),
            ('empleos_por_cuenta_ajena_2014_tercer_trimestre', 174, 'numeric'),
            ('empleos_por_cuenta_propia_2014_tercer_trimestre', 175, 'numeric'),
            ('total_2014_segundo_trimestre', 176, 'numeric'),
            ('empleos_por_cuenta_ajena_2014_segundo_trimestre', 177, 'numeric'),
            ('empleos_por_cuenta_propia_2014_segundo_trimestre', 178, 'numeric'),
            ('total_2014_primer_trimestre', 179, 'numeric'),
            ('empleos_por_cuenta_ajena_2014_primer_trimestre', 180, 'numeric'),
            ('empleos_por_cuenta_propia_2014_primer_trimestre', 181, 'numeric'),
            ('total_2013', 182, 'numeric'),
            ('empleos_por_cuenta_ajena_2013', 183, 'numeric'),
            ('empleos_por_cuenta_propia_2013', 184, 'numeric'),
            ('total_2013_cuarto_trimestre', 185, 'numeric'),
            ('empleos_por_cuenta_ajena_2013_cuarto_trimestre', 186, 'numeric'),
            ('empleos_por_cuenta_propia_2013_cuarto_trimestre', 187, 'numeric'),
            ('total_2013_tercer_trimestre', 188, 'numeric'),
            ('empleos_por_cuenta_ajena_2013_tercer_trimestre', 189, 'numeric'),
            ('empleos_por_cuenta_propia_2013_tercer_trimestre', 190, 'numeric'),
            ('total_2013_segundo_trimestre', 191, 'numeric'),
            ('empleos_por_cuenta_ajena_2013_segundo_trimestre', 192, 'numeric'),
            ('empleos_por_cuenta_propia_2013_segundo_trimestre', 193, 'numeric'),
            ('total_2013_primer_trimestre', 194, 'numeric'),
            ('empleos_por_cuenta_ajena_2013_primer_trimestre', 195, 'numeric'),
            ('empleos_por_cuenta_propia_2013_primer_trimestre', 196, 'numeric'),
            ('total_2012', 197, 'numeric'),
            ('empleos_por_cuenta_ajena_2012', 198, 'numeric'),
            ('empleos_por_cuenta_propia_2012', 199, 'numeric'),
            ('total_2012_cuarto_trimestre', 200, 'numeric'),
            ('empleos_por_cuenta_ajena_2012_cuarto_trimestre', 201, 'numeric'),
            ('empleos_por_cuenta_propia_2012_cuarto_trimestre', 202, 'numeric'),
            ('total_2012_tercer_trimestre', 203, 'numeric'),
            ('empleos_por_cuenta_ajena_2012_tercer_trimestre', 204, 'numeric'),
            ('empleos_por_cuenta_propia_2012_tercer_trimestre', 205, 'numeric'),
            ('total_2012_segundo_trimestre', 206, 'numeric'),
            ('empleos_por_cuenta_ajena_2012_segundo_trimestre', 207, 'numeric'),
            ('empleos_por_cuenta_propia_2012_segundo_trimestre', 208, 'numeric'),
            ('total_2012_primer_trimestre', 209, 'numeric'),
            ('empleos_por_cuenta_ajena_2012_primer_trimestre', 210, 'numeric'),
            ('empleos_por_cuenta_propia_2012_primer_trimestre', 211, 'numeric'),
            ('total_2011', 212, 'numeric'),
            ('empleos_por_cuenta_ajena_2011', 213, 'numeric'),
            ('empleos_por_cuenta_propia_2011', 214, 'numeric'),
            ('total_2011_cuarto_trimestre', 215, 'numeric'),
            ('empleos_por_cuenta_ajena_2011_cuarto_trimestre', 216, 'numeric'),
            ('empleos_por_cuenta_propia_2011_cuarto_trimestre', 217, 'numeric'),
            ('total_2011_tercer_trimestre', 218, 'numeric'),
            ('empleos_por_cuenta_ajena_2011_tercer_trimestre', 219, 'numeric'),
            ('empleos_por_cuenta_propia_2011_tercer_trimestre', 220, 'numeric'),
            ('total_2011_segundo_trimestre', 221, 'numeric'),
            ('empleos_por_cuenta_ajena_2011_segundo_trimestre', 222, 'numeric'),
            ('empleos_por_cuenta_propia_2011_segundo_trimestre', 223, 'numeric'),
            ('total_2011_primer_trimestre', 224, 'numeric'),
            ('empleos_por_cuenta_ajena_2011_primer_trimestre', 225, 'numeric'),
            ('empleos_por_cuenta_propia_2011_primer_trimestre', 226, 'numeric'),
            ('total_2010', 227, 'numeric'),
            ('empleos_por_cuenta_ajena_2010', 228, 'numeric'),
            ('empleos_por_cuenta_propia_2010', 229, 'numeric'),
            ('total_2010_cuarto_trimestre', 230, 'numeric'),
            ('empleos_por_cuenta_ajena_2010_cuarto_trimestre', 231, 'numeric'),
            ('empleos_por_cuenta_propia_2010_cuarto_trimestre', 232, 'numeric'),
            ('total_2010_tercer_trimestre', 233, 'numeric'),
            ('empleos_por_cuenta_ajena_2010_tercer_trimestre', 234, 'numeric'),
            ('empleos_por_cuenta_propia_2010_tercer_trimestre', 235, 'numeric'),
            ('total_2010_segundo_trimestre', 236, 'numeric'),
            ('empleos_por_cuenta_ajena_2010_segundo_trimestre', 237, 'numeric'),
            ('empleos_por_cuenta_propia_2010_segundo_trimestre', 238, 'numeric'),
            ('total_2010_primer_trimestre', 239, 'numeric'),
            ('empleos_por_cuenta_ajena_2010_primer_trimestre', 240, 'numeric'),
            ('empleos_por_cuenta_propia_2010_primer_trimestre', 241, 'numeric'),
            ('total_2009', 242, 'numeric'),
            ('empleos_por_cuenta_ajena_2009', 243, 'numeric'),
            ('empleos_por_cuenta_propia_2009', 244, 'numeric'),
            ('total_2009_cuarto_trimestre', 245, 'numeric'),
            ('empleos_por_cuenta_ajena_2009_cuarto_trimestre', 246, 'numeric'),
            ('empleos_por_cuenta_propia_2009_cuarto_trimestre', 247, 'numeric'),
            ('total_2009_tercer_trimestre', 248, 'numeric'),
            ('empleos_por_cuenta_ajena_2009_tercer_trimestre', 249, 'numeric'),
            ('empleos_por_cuenta_propia_2009_tercer_trimestre', 250, 'numeric'),
            ('total_2009_segundo_trimestre', 251, 'numeric'),
            ('empleos_por_cuenta_ajena_2009_segundo_trimestre', 252, 'numeric'),
            ('empleos_por_cuenta_propia_2009_segundo_trimestre', 253, 'numeric'),
            ('total_2009_primer_trimestre', 254, 'numeric'),
            ('empleos_por_cuenta_ajena_2009_primer_trimestre', 255, 'numeric'),
            ('empleos_por_cuenta_propia_2009_primer_trimestre', 256, 'numeric'),
        ],
    },
    {
        "dataset_name": 'istac_employment_divisions',
        "file_name": '(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv',
        "table_name": 'src.istac_employment_divisions_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'quarterly',
        "time_field": None,
        "series_keys": ['activity'],
        "value_fields": ['empleo_2008_cuarto_trimestre', 'empleo_2008_tercer_trimestre', 'empleo_2008_segundo_trimestre', 'empleo_2008_primer_trimestre', 'empleo_2007_cuarto_trimestre', 'empleo_2007_tercer_trimestre', 'empleo_2007_segundo_trimestre', 'empleo_2007_primer_trimestre', 'empleo_2006_cuarto_trimestre', 'empleo_2006_tercer_trimestre', 'empleo_2006_segundo_trimestre', 'empleo_2006_primer_trimestre', 'empleo_2005_cuarto_trimestre', 'empleo_2005_tercer_trimestre', 'empleo_2005_segundo_trimestre', 'empleo_2005_primer_trimestre', 'empleo_2004_cuarto_trimestre', 'empleo_2004_tercer_trimestre', 'empleo_2004_segundo_trimestre', 'empleo_2004_primer_trimestre', 'empleo_2003_cuarto_trimestre', 'empleo_2003_tercer_trimestre', 'empleo_2003_segundo_trimestre', 'empleo_2003_primer_trimestre', 'empleo_2002_cuarto_trimestre', 'empleo_2002_tercer_trimestre', 'empleo_2002_segundo_trimestre', 'empleo_2002_primer_trimestre', 'empleo_2001_cuarto_trimestre', 'empleo_2001_tercer_trimestre', 'empleo_2001_segundo_trimestre', 'empleo_2001_primer_trimestre', 'empleo_2000_cuarto_trimestre', 'empleo_2000_tercer_trimestre', 'empleo_2000_segundo_trimestre', 'empleo_2000_primer_trimestre', 'empleo_1999_cuarto_trimestre', 'empleo_1999_tercer_trimestre', 'empleo_1999_segundo_trimestre', 'empleo_1999_primer_trimestre'],
        "columns": [
            ('activity', 0, 'text'),
            ('empleo_2008_cuarto_trimestre', 1, 'numeric'),
            ('empleo_2008_tercer_trimestre', 2, 'numeric'),
            ('empleo_2008_segundo_trimestre', 3, 'numeric'),
            ('empleo_2008_primer_trimestre', 4, 'numeric'),
            ('empleo_2007_cuarto_trimestre', 5, 'numeric'),
            ('empleo_2007_tercer_trimestre', 6, 'numeric'),
            ('empleo_2007_segundo_trimestre', 7, 'numeric'),
            ('empleo_2007_primer_trimestre', 8, 'numeric'),
            ('empleo_2006_cuarto_trimestre', 9, 'numeric'),
            ('empleo_2006_tercer_trimestre', 10, 'numeric'),
            ('empleo_2006_segundo_trimestre', 11, 'numeric'),
            ('empleo_2006_primer_trimestre', 12, 'numeric'),
            ('empleo_2005_cuarto_trimestre', 13, 'numeric'),
            ('empleo_2005_tercer_trimestre', 14, 'numeric'),
            ('empleo_2005_segundo_trimestre', 15, 'numeric'),
            ('empleo_2005_primer_trimestre', 16, 'numeric'),
            ('empleo_2004_cuarto_trimestre', 17, 'numeric'),
            ('empleo_2004_tercer_trimestre', 18, 'numeric'),
            ('empleo_2004_segundo_trimestre', 19, 'numeric'),
            ('empleo_2004_primer_trimestre', 20, 'numeric'),
            ('empleo_2003_cuarto_trimestre', 21, 'numeric'),
            ('empleo_2003_tercer_trimestre', 22, 'numeric'),
            ('empleo_2003_segundo_trimestre', 23, 'numeric'),
            ('empleo_2003_primer_trimestre', 24, 'numeric'),
            ('empleo_2002_cuarto_trimestre', 25, 'numeric'),
            ('empleo_2002_tercer_trimestre', 26, 'numeric'),
            ('empleo_2002_segundo_trimestre', 27, 'numeric'),
            ('empleo_2002_primer_trimestre', 28, 'numeric'),
            ('empleo_2001_cuarto_trimestre', 29, 'numeric'),
            ('empleo_2001_tercer_trimestre', 30, 'numeric'),
            ('empleo_2001_segundo_trimestre', 31, 'numeric'),
            ('empleo_2001_primer_trimestre', 32, 'numeric'),
            ('empleo_2000_cuarto_trimestre', 33, 'numeric'),
            ('empleo_2000_tercer_trimestre', 34, 'numeric'),
            ('empleo_2000_segundo_trimestre', 35, 'numeric'),
            ('empleo_2000_primer_trimestre', 36, 'numeric'),
            ('empleo_1999_cuarto_trimestre', 37, 'numeric'),
            ('empleo_1999_tercer_trimestre', 38, 'numeric'),
            ('empleo_1999_segundo_trimestre', 39, 'numeric'),
            ('empleo_1999_primer_trimestre', 40, 'numeric'),
        ],
    },
    {
        "dataset_name": 'istac_employment_status',
        "file_name": '(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv',
        "table_name": 'src.istac_employment_status_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'quarterly',
        "time_field": None,
        "series_keys": ['geography', 'employment_status'],
        "value_fields": ['empleo_2026_segundo_trimestre', 'empleo_2026_primer_trimestre', 'empleo_2025', 'empleo_2025_cuarto_trimestre', 'empleo_2025_tercer_trimestre', 'empleo_2025_segundo_trimestre', 'empleo_2025_primer_trimestre', 'empleo_2024', 'empleo_2024_cuarto_trimestre', 'empleo_2024_tercer_trimestre', 'empleo_2024_segundo_trimestre', 'empleo_2024_primer_trimestre', 'empleo_2023', 'empleo_2023_cuarto_trimestre', 'empleo_2023_tercer_trimestre', 'empleo_2023_segundo_trimestre', 'empleo_2023_primer_trimestre', 'empleo_2022', 'empleo_2022_cuarto_trimestre', 'empleo_2022_tercer_trimestre', 'empleo_2022_segundo_trimestre', 'empleo_2022_primer_trimestre', 'empleo_2021', 'empleo_2021_cuarto_trimestre', 'empleo_2021_tercer_trimestre', 'empleo_2021_segundo_trimestre', 'empleo_2021_primer_trimestre', 'empleo_2020', 'empleo_2020_cuarto_trimestre', 'empleo_2020_tercer_trimestre', 'empleo_2020_segundo_trimestre', 'empleo_2020_primer_trimestre', 'empleo_2019', 'empleo_2019_cuarto_trimestre', 'empleo_2019_tercer_trimestre', 'empleo_2019_segundo_trimestre', 'empleo_2019_primer_trimestre', 'empleo_2018', 'empleo_2018_cuarto_trimestre', 'empleo_2018_tercer_trimestre', 'empleo_2018_segundo_trimestre', 'empleo_2018_primer_trimestre', 'empleo_2017', 'empleo_2017_cuarto_trimestre', 'empleo_2017_tercer_trimestre', 'empleo_2017_segundo_trimestre', 'empleo_2017_primer_trimestre', 'empleo_2016', 'empleo_2016_cuarto_trimestre', 'empleo_2016_tercer_trimestre', 'empleo_2016_segundo_trimestre', 'empleo_2016_primer_trimestre', 'empleo_2015', 'empleo_2015_cuarto_trimestre', 'empleo_2015_tercer_trimestre', 'empleo_2015_segundo_trimestre', 'empleo_2015_primer_trimestre', 'empleo_2014', 'empleo_2014_cuarto_trimestre', 'empleo_2014_tercer_trimestre', 'empleo_2014_segundo_trimestre', 'empleo_2014_primer_trimestre', 'empleo_2013', 'empleo_2013_cuarto_trimestre', 'empleo_2013_tercer_trimestre', 'empleo_2013_segundo_trimestre', 'empleo_2013_primer_trimestre', 'empleo_2012', 'empleo_2012_cuarto_trimestre', 'empleo_2012_tercer_trimestre', 'empleo_2012_segundo_trimestre', 'empleo_2012_primer_trimestre', 'empleo_2011', 'empleo_2011_cuarto_trimestre', 'empleo_2011_tercer_trimestre', 'empleo_2011_segundo_trimestre', 'empleo_2011_primer_trimestre', 'empleo_2010', 'empleo_2010_cuarto_trimestre', 'empleo_2010_tercer_trimestre', 'empleo_2010_segundo_trimestre', 'empleo_2010_primer_trimestre', 'empleo_2009', 'empleo_2009_cuarto_trimestre', 'empleo_2009_tercer_trimestre', 'empleo_2009_segundo_trimestre', 'empleo_2009_primer_trimestre', 'empleo_2008', 'empleo_2008_cuarto_trimestre', 'empleo_2008_tercer_trimestre', 'empleo_2008_segundo_trimestre', 'empleo_2008_primer_trimestre', 'empleo_2007', 'empleo_2007_cuarto_trimestre', 'empleo_2007_tercer_trimestre', 'empleo_2007_segundo_trimestre', 'empleo_2007_primer_trimestre', 'empleo_2006', 'empleo_2006_cuarto_trimestre', 'empleo_2006_tercer_trimestre', 'empleo_2006_segundo_trimestre', 'empleo_2006_primer_trimestre', 'empleo_2005', 'empleo_2005_cuarto_trimestre', 'empleo_2005_tercer_trimestre', 'empleo_2005_segundo_trimestre', 'empleo_2005_primer_trimestre', 'empleo_2004', 'empleo_2004_cuarto_trimestre', 'empleo_2004_tercer_trimestre', 'empleo_2004_segundo_trimestre', 'empleo_2004_primer_trimestre', 'empleo_2003', 'empleo_2003_cuarto_trimestre', 'empleo_2003_tercer_trimestre', 'empleo_2003_segundo_trimestre', 'empleo_2003_primer_trimestre', 'empleo_2002', 'empleo_2002_cuarto_trimestre', 'empleo_2002_tercer_trimestre', 'empleo_2002_segundo_trimestre', 'empleo_2002_primer_trimestre', 'empleo_2001', 'empleo_2001_cuarto_trimestre', 'empleo_2001_tercer_trimestre', 'empleo_2001_segundo_trimestre', 'empleo_2001_primer_trimestre', 'empleo_2000', 'empleo_2000_cuarto_trimestre', 'empleo_2000_tercer_trimestre', 'empleo_2000_segundo_trimestre', 'empleo_2000_primer_trimestre', 'empleo_1999', 'empleo_1999_cuarto_trimestre', 'empleo_1999_tercer_trimestre', 'empleo_1999_segundo_trimestre', 'empleo_1999_primer_trimestre'],
        "columns": [
            ('geography', 0, 'text'),
            ('employment_status', 1, 'text'),
            ('empleo_2026_segundo_trimestre', 2, 'numeric'),
            ('empleo_2026_primer_trimestre', 3, 'numeric'),
            ('empleo_2025', 4, 'numeric'),
            ('empleo_2025_cuarto_trimestre', 5, 'numeric'),
            ('empleo_2025_tercer_trimestre', 6, 'numeric'),
            ('empleo_2025_segundo_trimestre', 7, 'numeric'),
            ('empleo_2025_primer_trimestre', 8, 'numeric'),
            ('empleo_2024', 9, 'numeric'),
            ('empleo_2024_cuarto_trimestre', 10, 'numeric'),
            ('empleo_2024_tercer_trimestre', 11, 'numeric'),
            ('empleo_2024_segundo_trimestre', 12, 'numeric'),
            ('empleo_2024_primer_trimestre', 13, 'numeric'),
            ('empleo_2023', 14, 'numeric'),
            ('empleo_2023_cuarto_trimestre', 15, 'numeric'),
            ('empleo_2023_tercer_trimestre', 16, 'numeric'),
            ('empleo_2023_segundo_trimestre', 17, 'numeric'),
            ('empleo_2023_primer_trimestre', 18, 'numeric'),
            ('empleo_2022', 19, 'numeric'),
            ('empleo_2022_cuarto_trimestre', 20, 'numeric'),
            ('empleo_2022_tercer_trimestre', 21, 'numeric'),
            ('empleo_2022_segundo_trimestre', 22, 'numeric'),
            ('empleo_2022_primer_trimestre', 23, 'numeric'),
            ('empleo_2021', 24, 'numeric'),
            ('empleo_2021_cuarto_trimestre', 25, 'numeric'),
            ('empleo_2021_tercer_trimestre', 26, 'numeric'),
            ('empleo_2021_segundo_trimestre', 27, 'numeric'),
            ('empleo_2021_primer_trimestre', 28, 'numeric'),
            ('empleo_2020', 29, 'numeric'),
            ('empleo_2020_cuarto_trimestre', 30, 'numeric'),
            ('empleo_2020_tercer_trimestre', 31, 'numeric'),
            ('empleo_2020_segundo_trimestre', 32, 'numeric'),
            ('empleo_2020_primer_trimestre', 33, 'numeric'),
            ('empleo_2019', 34, 'numeric'),
            ('empleo_2019_cuarto_trimestre', 35, 'numeric'),
            ('empleo_2019_tercer_trimestre', 36, 'numeric'),
            ('empleo_2019_segundo_trimestre', 37, 'numeric'),
            ('empleo_2019_primer_trimestre', 38, 'numeric'),
            ('empleo_2018', 39, 'numeric'),
            ('empleo_2018_cuarto_trimestre', 40, 'numeric'),
            ('empleo_2018_tercer_trimestre', 41, 'numeric'),
            ('empleo_2018_segundo_trimestre', 42, 'numeric'),
            ('empleo_2018_primer_trimestre', 43, 'numeric'),
            ('empleo_2017', 44, 'numeric'),
            ('empleo_2017_cuarto_trimestre', 45, 'numeric'),
            ('empleo_2017_tercer_trimestre', 46, 'numeric'),
            ('empleo_2017_segundo_trimestre', 47, 'numeric'),
            ('empleo_2017_primer_trimestre', 48, 'numeric'),
            ('empleo_2016', 49, 'numeric'),
            ('empleo_2016_cuarto_trimestre', 50, 'numeric'),
            ('empleo_2016_tercer_trimestre', 51, 'numeric'),
            ('empleo_2016_segundo_trimestre', 52, 'numeric'),
            ('empleo_2016_primer_trimestre', 53, 'numeric'),
            ('empleo_2015', 54, 'numeric'),
            ('empleo_2015_cuarto_trimestre', 55, 'numeric'),
            ('empleo_2015_tercer_trimestre', 56, 'numeric'),
            ('empleo_2015_segundo_trimestre', 57, 'numeric'),
            ('empleo_2015_primer_trimestre', 58, 'numeric'),
            ('empleo_2014', 59, 'numeric'),
            ('empleo_2014_cuarto_trimestre', 60, 'numeric'),
            ('empleo_2014_tercer_trimestre', 61, 'numeric'),
            ('empleo_2014_segundo_trimestre', 62, 'numeric'),
            ('empleo_2014_primer_trimestre', 63, 'numeric'),
            ('empleo_2013', 64, 'numeric'),
            ('empleo_2013_cuarto_trimestre', 65, 'numeric'),
            ('empleo_2013_tercer_trimestre', 66, 'numeric'),
            ('empleo_2013_segundo_trimestre', 67, 'numeric'),
            ('empleo_2013_primer_trimestre', 68, 'numeric'),
            ('empleo_2012', 69, 'numeric'),
            ('empleo_2012_cuarto_trimestre', 70, 'numeric'),
            ('empleo_2012_tercer_trimestre', 71, 'numeric'),
            ('empleo_2012_segundo_trimestre', 72, 'numeric'),
            ('empleo_2012_primer_trimestre', 73, 'numeric'),
            ('empleo_2011', 74, 'numeric'),
            ('empleo_2011_cuarto_trimestre', 75, 'numeric'),
            ('empleo_2011_tercer_trimestre', 76, 'numeric'),
            ('empleo_2011_segundo_trimestre', 77, 'numeric'),
            ('empleo_2011_primer_trimestre', 78, 'numeric'),
            ('empleo_2010', 79, 'numeric'),
            ('empleo_2010_cuarto_trimestre', 80, 'numeric'),
            ('empleo_2010_tercer_trimestre', 81, 'numeric'),
            ('empleo_2010_segundo_trimestre', 82, 'numeric'),
            ('empleo_2010_primer_trimestre', 83, 'numeric'),
            ('empleo_2009', 84, 'numeric'),
            ('empleo_2009_cuarto_trimestre', 85, 'numeric'),
            ('empleo_2009_tercer_trimestre', 86, 'numeric'),
            ('empleo_2009_segundo_trimestre', 87, 'numeric'),
            ('empleo_2009_primer_trimestre', 88, 'numeric'),
            ('empleo_2008', 89, 'numeric'),
            ('empleo_2008_cuarto_trimestre', 90, 'numeric'),
            ('empleo_2008_tercer_trimestre', 91, 'numeric'),
            ('empleo_2008_segundo_trimestre', 92, 'numeric'),
            ('empleo_2008_primer_trimestre', 93, 'numeric'),
            ('empleo_2007', 94, 'numeric'),
            ('empleo_2007_cuarto_trimestre', 95, 'numeric'),
            ('empleo_2007_tercer_trimestre', 96, 'numeric'),
            ('empleo_2007_segundo_trimestre', 97, 'numeric'),
            ('empleo_2007_primer_trimestre', 98, 'numeric'),
            ('empleo_2006', 99, 'numeric'),
            ('empleo_2006_cuarto_trimestre', 100, 'numeric'),
            ('empleo_2006_tercer_trimestre', 101, 'numeric'),
            ('empleo_2006_segundo_trimestre', 102, 'numeric'),
            ('empleo_2006_primer_trimestre', 103, 'numeric'),
            ('empleo_2005', 104, 'numeric'),
            ('empleo_2005_cuarto_trimestre', 105, 'numeric'),
            ('empleo_2005_tercer_trimestre', 106, 'numeric'),
            ('empleo_2005_segundo_trimestre', 107, 'numeric'),
            ('empleo_2005_primer_trimestre', 108, 'numeric'),
            ('empleo_2004', 109, 'numeric'),
            ('empleo_2004_cuarto_trimestre', 110, 'numeric'),
            ('empleo_2004_tercer_trimestre', 111, 'numeric'),
            ('empleo_2004_segundo_trimestre', 112, 'numeric'),
            ('empleo_2004_primer_trimestre', 113, 'numeric'),
            ('empleo_2003', 114, 'numeric'),
            ('empleo_2003_cuarto_trimestre', 115, 'numeric'),
            ('empleo_2003_tercer_trimestre', 116, 'numeric'),
            ('empleo_2003_segundo_trimestre', 117, 'numeric'),
            ('empleo_2003_primer_trimestre', 118, 'numeric'),
            ('empleo_2002', 119, 'numeric'),
            ('empleo_2002_cuarto_trimestre', 120, 'numeric'),
            ('empleo_2002_tercer_trimestre', 121, 'numeric'),
            ('empleo_2002_segundo_trimestre', 122, 'numeric'),
            ('empleo_2002_primer_trimestre', 123, 'numeric'),
            ('empleo_2001', 124, 'numeric'),
            ('empleo_2001_cuarto_trimestre', 125, 'numeric'),
            ('empleo_2001_tercer_trimestre', 126, 'numeric'),
            ('empleo_2001_segundo_trimestre', 127, 'numeric'),
            ('empleo_2001_primer_trimestre', 128, 'numeric'),
            ('empleo_2000', 129, 'numeric'),
            ('empleo_2000_cuarto_trimestre', 130, 'numeric'),
            ('empleo_2000_tercer_trimestre', 131, 'numeric'),
            ('empleo_2000_segundo_trimestre', 132, 'numeric'),
            ('empleo_2000_primer_trimestre', 133, 'numeric'),
            ('empleo_1999', 134, 'numeric'),
            ('empleo_1999_cuarto_trimestre', 135, 'numeric'),
            ('empleo_1999_tercer_trimestre', 136, 'numeric'),
            ('empleo_1999_segundo_trimestre', 137, 'numeric'),
            ('empleo_1999_primer_trimestre', 138, 'numeric'),
        ],
    },
    {
        "dataset_name": 'foreign_affiliates_cnae',
        "file_name": '(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv',
        "table_name": 'src.foreign_affiliates_cnae_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ';',
        "header_rows": 2,
        "decimal_style": 'dot',
        "temporal_type": 'monthly',
        "time_field": 'periodo',
        "series_keys": ['ccaa'],
        "value_fields": ['total_rg_y_reta_total_regimen_total_ue_y_no_ue', 'total_rg_y_reta_total_regimen_union_europea', 'total_rg_y_reta_total_regimen_no_union', 'total_rg_y_reta_r_e_autonomos_no_seta_total_ue_y_no_ue', 'total_rg_y_reta_r_e_autonomos_no_seta_union_europea', 'total_rg_y_reta_r_e_autonomos_no_seta_no_union', 'total_rg_y_reta_reg_e_autonomos_seta_total_ue_y_no_ue', 'total_rg_y_reta_reg_e_autonomos_seta_union_europea', 'total_rg_y_reta_reg_e_autonomos_seta_no_union', 'total_rg_y_reta_regimen_general_total_ue_y_no_ue', 'total_rg_y_reta_regimen_general_union_europea', 'total_rg_y_reta_regimen_general_no_union', 'a_agricultura_ganadera_silvicultura_y_pesca_total_regi_c86a757e', 'a_agricultura_ganadera_silvicultura_y_pesca_total_regi_ac41c250', 'a_agricultura_ganadera_silvicultura_y_pesca_total_regi_9f8af873', 'a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_36d1ef26', 'a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_41a81308', 'a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_e4050695', 'a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_cbf9f456', 'a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_72b570f5', 'a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_13065814', 'a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_2b144925', 'a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_26f1c619', 'a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_a7624079', 'b_industrias_extractivas_total_regimen_total_ue_y_no_ue', 'b_industrias_extractivas_total_regimen_union_europea', 'b_industrias_extractivas_total_regimen_no_union', 'b_industrias_extractivas_r_e_autonomos_no_seta_total_ue_y_no_ue', 'b_industrias_extractivas_r_e_autonomos_no_seta_union_europea', 'b_industrias_extractivas_r_e_autonomos_no_seta_no_union', 'b_industrias_extractivas_reg_e_autonomos_seta_total_ue_y_no_ue', 'b_industrias_extractivas_reg_e_autonomos_seta_union_europea', 'b_industrias_extractivas_reg_e_autonomos_seta_no_union', 'b_industrias_extractivas_regimen_general_total_ue_y_no_ue', 'b_industrias_extractivas_regimen_general_union_europea', 'b_industrias_extractivas_regimen_general_no_union', 'c_industria_manufacturera_total_regimen_total_ue_y_no_ue', 'c_industria_manufacturera_total_regimen_union_europea', 'c_industria_manufacturera_total_regimen_no_union', 'c_industria_manufacturera_r_e_autonomos_no_seta_total_769b9e89', 'c_industria_manufacturera_r_e_autonomos_no_seta_union_europea', 'c_industria_manufacturera_r_e_autonomos_no_seta_no_union', 'c_industria_manufacturera_reg_e_autonomos_seta_total_ue_y_no_ue', 'c_industria_manufacturera_reg_e_autonomos_seta_union_europea', 'c_industria_manufacturera_reg_e_autonomos_seta_no_union', 'c_industria_manufacturera_regimen_general_total_ue_y_no_ue', 'c_industria_manufacturera_regimen_general_union_europea', 'c_industria_manufacturera_regimen_general_no_union', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_66c9ca19', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_8bc01880', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_0503f5c1', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_d9779513', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_84138853', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_13ef912b', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_5b1f3cd7', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_5cdd6b5d', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_a6a7ed01', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_aa7ab2c1', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_792e9c60', 'd_suministro_de_energia_electrica_gas_vapor_y_aire_aco_7f8642a5', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_2ae59acc', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_348a6da9', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_d5b2750f', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_ef6ae68d', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_8e704702', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_23825dc3', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_8ef7de79', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_c78a2072', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_c5a37613', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_f60ed31e', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_919aba74', 'e_suministro_de_agua_actividades_de_saneamiento_gestin_f11ebf12', 'f_construccin_total_regimen_total_ue_y_no_ue', 'f_construccin_total_regimen_union_europea', 'f_construccin_total_regimen_no_union', 'f_construccin_r_e_autonomos_no_seta_total_ue_y_no_ue', 'f_construccin_r_e_autonomos_no_seta_union_europea', 'f_construccin_r_e_autonomos_no_seta_no_union', 'f_construccin_reg_e_autonomos_seta_total_ue_y_no_ue', 'f_construccin_reg_e_autonomos_seta_union_europea', 'f_construccin_reg_e_autonomos_seta_no_union', 'f_construccin_regimen_general_total_ue_y_no_ue', 'f_construccin_regimen_general_union_europea', 'f_construccin_regimen_general_no_union', 'g_comercio_al_por_mayor_y_al_por_menor_total_regimen_t_a53d2a85', 'g_comercio_al_por_mayor_y_al_por_menor_total_regimen_u_1cc44bc3', 'g_comercio_al_por_mayor_y_al_por_menor_total_regimen_no_union', 'g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_7e830c82', 'g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_a7a41331', 'g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_eb5d27f1', 'g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_c40ab3b8', 'g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_c073afa4', 'g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_1a844c57', 'g_comercio_al_por_mayor_y_al_por_menor_regimen_general_89f4b669', 'g_comercio_al_por_mayor_y_al_por_menor_regimen_general_eac4f084', 'g_comercio_al_por_mayor_y_al_por_menor_regimen_general_no_union', 'h_transporte_y_almacenamiento_total_regimen_total_ue_y_no_ue', 'h_transporte_y_almacenamiento_total_regimen_union_europea', 'h_transporte_y_almacenamiento_total_regimen_no_union', 'h_transporte_y_almacenamiento_r_e_autonomos_no_seta_to_6f7f0c8c', 'h_transporte_y_almacenamiento_r_e_autonomos_no_seta_un_40d870fc', 'h_transporte_y_almacenamiento_r_e_autonomos_no_seta_no_union', 'h_transporte_y_almacenamiento_reg_e_autonomos_seta_tot_aee1802b', 'h_transporte_y_almacenamiento_reg_e_autonomos_seta_uni_b2e33c27', 'h_transporte_y_almacenamiento_reg_e_autonomos_seta_no_union', 'h_transporte_y_almacenamiento_regimen_general_total_ue_y_no_ue', 'h_transporte_y_almacenamiento_regimen_general_union_europea', 'h_transporte_y_almacenamiento_regimen_general_no_union', 'i_hostelera_total_regimen_total_ue_y_no_ue', 'i_hostelera_total_regimen_union_europea', 'i_hostelera_total_regimen_no_union', 'i_hostelera_r_e_autonomos_no_seta_total_ue_y_no_ue', 'i_hostelera_r_e_autonomos_no_seta_union_europea', 'i_hostelera_r_e_autonomos_no_seta_no_union', 'i_hostelera_reg_e_autonomos_seta_total_ue_y_no_ue', 'i_hostelera_reg_e_autonomos_seta_union_europea', 'i_hostelera_reg_e_autonomos_seta_no_union', 'i_hostelera_regimen_general_total_ue_y_no_ue', 'i_hostelera_regimen_general_union_europea', 'i_hostelera_regimen_general_no_union', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_d010762d', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_5a74561b', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_84cee81e', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_3ba7f8a8', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_15d135ea', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_9da5469e', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_4fbe8fa4', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_106dc4a7', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_2542c800', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_f15b851d', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_a26b3378', 'j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_5a36de72', 'k_telecomunicaciones_programacin_informtica_consultora_d0982e63', 'k_telecomunicaciones_programacin_informtica_consultora_e800632f', 'k_telecomunicaciones_programacin_informtica_consultora_dde7652b', 'k_telecomunicaciones_programacin_informtica_consultora_213dc215', 'k_telecomunicaciones_programacin_informtica_consultora_6bcebcbb', 'k_telecomunicaciones_programacin_informtica_consultora_511cbaff', 'k_telecomunicaciones_programacin_informtica_consultora_bf1d18e0', 'k_telecomunicaciones_programacin_informtica_consultora_a9939f17', 'k_telecomunicaciones_programacin_informtica_consultora_d1c4ba9f', 'k_telecomunicaciones_programacin_informtica_consultora_1a4e4a6d', 'k_telecomunicaciones_programacin_informtica_consultora_f505c98b', 'k_telecomunicaciones_programacin_informtica_consultora_73b9b58c', 'l_actividades_financieras_y_de_seguros_total_regimen_t_ab32213f', 'l_actividades_financieras_y_de_seguros_total_regimen_u_d6039741', 'l_actividades_financieras_y_de_seguros_total_regimen_no_union', 'l_actividades_financieras_y_de_seguros_r_e_autonomos_n_0a0e7ae8', 'l_actividades_financieras_y_de_seguros_r_e_autonomos_n_21d682d3', 'l_actividades_financieras_y_de_seguros_r_e_autonomos_n_baa86b30', 'l_actividades_financieras_y_de_seguros_reg_e_autonomos_93c7dc34', 'l_actividades_financieras_y_de_seguros_reg_e_autonomos_c9db9737', 'l_actividades_financieras_y_de_seguros_reg_e_autonomos_73c72e78', 'l_actividades_financieras_y_de_seguros_regimen_general_d1a398e5', 'l_actividades_financieras_y_de_seguros_regimen_general_2a18de0f', 'l_actividades_financieras_y_de_seguros_regimen_general_no_union', 'm_actividades_inmobiliarias_total_regimen_total_ue_y_no_ue', 'm_actividades_inmobiliarias_total_regimen_union_europea', 'm_actividades_inmobiliarias_total_regimen_no_union', 'm_actividades_inmobiliarias_r_e_autonomos_no_seta_tota_5cbb832b', 'm_actividades_inmobiliarias_r_e_autonomos_no_seta_union_europea', 'm_actividades_inmobiliarias_r_e_autonomos_no_seta_no_union', 'm_actividades_inmobiliarias_reg_e_autonomos_seta_total_3784bbb0', 'm_actividades_inmobiliarias_reg_e_autonomos_seta_union_europea', 'm_actividades_inmobiliarias_reg_e_autonomos_seta_no_union', 'm_actividades_inmobiliarias_regimen_general_total_ue_y_no_ue', 'm_actividades_inmobiliarias_regimen_general_union_europea', 'm_actividades_inmobiliarias_regimen_general_no_union', 'n_actividades_profesionales_cientficas_y_tcnicas_total_40f76b93', 'n_actividades_profesionales_cientficas_y_tcnicas_total_f456bb5c', 'n_actividades_profesionales_cientficas_y_tcnicas_total_bab0bb75', 'n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_89cda3e6', 'n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_f33bfaab', 'n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_f30651cf', 'n_actividades_profesionales_cientficas_y_tcnicas_reg_e_d0732312', 'n_actividades_profesionales_cientficas_y_tcnicas_reg_e_f37abe62', 'n_actividades_profesionales_cientficas_y_tcnicas_reg_e_7bc9bbdc', 'n_actividades_profesionales_cientficas_y_tcnicas_regim_1395566a', 'n_actividades_profesionales_cientficas_y_tcnicas_regim_4461700f', 'n_actividades_profesionales_cientficas_y_tcnicas_regim_6ea486de', 'o_actividades_administrativas_y_servicios_auxiliares_t_4e04f725', 'o_actividades_administrativas_y_servicios_auxiliares_t_63da1a6d', 'o_actividades_administrativas_y_servicios_auxiliares_t_b4b8a5c0', 'o_actividades_administrativas_y_servicios_auxiliares_r_b3721462', 'o_actividades_administrativas_y_servicios_auxiliares_r_2af015d5', 'o_actividades_administrativas_y_servicios_auxiliares_r_1f672487', 'o_actividades_administrativas_y_servicios_auxiliares_r_282f0372', 'o_actividades_administrativas_y_servicios_auxiliares_r_c9d42787', 'o_actividades_administrativas_y_servicios_auxiliares_r_c544a449', 'o_actividades_administrativas_y_servicios_auxiliares_r_bd2cba12', 'o_actividades_administrativas_y_servicios_auxiliares_r_50696672', 'o_actividades_administrativas_y_servicios_auxiliares_r_ed444dd1', 'p_administracin_pblica_y_defensa_seguridad_social_obli_0177e7d8', 'p_administracin_pblica_y_defensa_seguridad_social_obli_13f6b177', 'p_administracin_pblica_y_defensa_seguridad_social_obli_d4197c40', 'p_administracin_pblica_y_defensa_seguridad_social_obli_e83e6b57', 'p_administracin_pblica_y_defensa_seguridad_social_obli_13e98690', 'p_administracin_pblica_y_defensa_seguridad_social_obli_d9834483', 'p_administracin_pblica_y_defensa_seguridad_social_obli_8ededd53', 'p_administracin_pblica_y_defensa_seguridad_social_obli_42ab8fbd', 'p_administracin_pblica_y_defensa_seguridad_social_obli_77c86968', 'p_administracin_pblica_y_defensa_seguridad_social_obli_8ab0c373', 'p_administracin_pblica_y_defensa_seguridad_social_obli_b89cabb5', 'p_administracin_pblica_y_defensa_seguridad_social_obli_8a45decf', 'q_educacin_total_regimen_total_ue_y_no_ue', 'q_educacin_total_regimen_union_europea', 'q_educacin_total_regimen_no_union', 'q_educacin_r_e_autonomos_no_seta_total_ue_y_no_ue', 'q_educacin_r_e_autonomos_no_seta_union_europea', 'q_educacin_r_e_autonomos_no_seta_no_union', 'q_educacin_reg_e_autonomos_seta_total_ue_y_no_ue', 'q_educacin_reg_e_autonomos_seta_union_europea', 'q_educacin_reg_e_autonomos_seta_no_union', 'q_educacin_regimen_general_total_ue_y_no_ue', 'q_educacin_regimen_general_union_europea', 'q_educacin_regimen_general_no_union', 'r_actividades_sanitarias_y_de_servicios_sociales_total_29aa544b', 'r_actividades_sanitarias_y_de_servicios_sociales_total_efc74c90', 'r_actividades_sanitarias_y_de_servicios_sociales_total_2fc6dd19', 'r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_5194529b', 'r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_9f38e60d', 'r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_a0a13fef', 'r_actividades_sanitarias_y_de_servicios_sociales_reg_e_af894175', 'r_actividades_sanitarias_y_de_servicios_sociales_reg_e_a1cbde0d', 'r_actividades_sanitarias_y_de_servicios_sociales_reg_e_530cf441', 'r_actividades_sanitarias_y_de_servicios_sociales_regim_1c87e25a', 'r_actividades_sanitarias_y_de_servicios_sociales_regim_e6b28bc7', 'r_actividades_sanitarias_y_de_servicios_sociales_regim_a017ac95', 's_actividades_artsticas_deportivas_y_de_entretenimient_13f6a7e6', 's_actividades_artsticas_deportivas_y_de_entretenimient_cc1eaeb5', 's_actividades_artsticas_deportivas_y_de_entretenimient_00ec14b5', 's_actividades_artsticas_deportivas_y_de_entretenimient_1c7119c9', 's_actividades_artsticas_deportivas_y_de_entretenimient_e456269a', 's_actividades_artsticas_deportivas_y_de_entretenimient_4a01afcc', 's_actividades_artsticas_deportivas_y_de_entretenimient_f07e8451', 's_actividades_artsticas_deportivas_y_de_entretenimient_573e2370', 's_actividades_artsticas_deportivas_y_de_entretenimient_dbfda443', 's_actividades_artsticas_deportivas_y_de_entretenimient_464aad44', 's_actividades_artsticas_deportivas_y_de_entretenimient_ee4681e2', 's_actividades_artsticas_deportivas_y_de_entretenimient_5c535f3e', 't_otros_servicios_total_regimen_total_ue_y_no_ue', 't_otros_servicios_total_regimen_union_europea', 't_otros_servicios_total_regimen_no_union', 't_otros_servicios_r_e_autonomos_no_seta_total_ue_y_no_ue', 't_otros_servicios_r_e_autonomos_no_seta_union_europea', 't_otros_servicios_r_e_autonomos_no_seta_no_union', 't_otros_servicios_reg_e_autonomos_seta_total_ue_y_no_ue', 't_otros_servicios_reg_e_autonomos_seta_union_europea', 't_otros_servicios_reg_e_autonomos_seta_no_union', 't_otros_servicios_regimen_general_total_ue_y_no_ue', 't_otros_servicios_regimen_general_union_europea', 't_otros_servicios_regimen_general_no_union', 'u_actividades_de_los_hogares_como_empleadores_de_perso_bc7e5ec9', 'u_actividades_de_los_hogares_como_empleadores_de_perso_3d7e96e1', 'u_actividades_de_los_hogares_como_empleadores_de_perso_6283f11f', 'u_actividades_de_los_hogares_como_empleadores_de_perso_07fe8f94', 'u_actividades_de_los_hogares_como_empleadores_de_perso_28246b88', 'u_actividades_de_los_hogares_como_empleadores_de_perso_c1548fa3', 'u_actividades_de_los_hogares_como_empleadores_de_perso_e78bc9f6', 'u_actividades_de_los_hogares_como_empleadores_de_perso_ac0cfca5', 'u_actividades_de_los_hogares_como_empleadores_de_perso_84b380d3', 'u_actividades_de_los_hogares_como_empleadores_de_perso_a8b5e8dc', 'u_actividades_de_los_hogares_como_empleadores_de_perso_47c96fc9', 'u_actividades_de_los_hogares_como_empleadores_de_perso_fab1e846', 'v_organismos_extraterritoriales_total_regimen_total_ue_y_no_ue', 'v_organismos_extraterritoriales_total_regimen_union_europea', 'v_organismos_extraterritoriales_total_regimen_no_union', 'v_organismos_extraterritoriales_r_e_autonomos_no_seta_fe750fe4', 'v_organismos_extraterritoriales_r_e_autonomos_no_seta_06224da8', 'v_organismos_extraterritoriales_r_e_autonomos_no_seta_no_union', 'v_organismos_extraterritoriales_reg_e_autonomos_seta_t_f1f68ea3', 'v_organismos_extraterritoriales_reg_e_autonomos_seta_u_188fa123', 'v_organismos_extraterritoriales_reg_e_autonomos_seta_no_union', 'v_organismos_extraterritoriales_regimen_general_total_0de9cba8', 'v_organismos_extraterritoriales_regimen_general_union_europea', 'v_organismos_extraterritoriales_regimen_general_no_union'],
        "columns": [
            ('periodo', 0, 'integer'),
            ('ccaa', 1, 'text'),
            ('total_rg_y_reta_total_regimen_total_ue_y_no_ue', 2, 'numeric'),
            ('total_rg_y_reta_total_regimen_union_europea', 3, 'numeric'),
            ('total_rg_y_reta_total_regimen_no_union', 4, 'numeric'),
            ('total_rg_y_reta_r_e_autonomos_no_seta_total_ue_y_no_ue', 5, 'numeric'),
            ('total_rg_y_reta_r_e_autonomos_no_seta_union_europea', 6, 'numeric'),
            ('total_rg_y_reta_r_e_autonomos_no_seta_no_union', 7, 'numeric'),
            ('total_rg_y_reta_reg_e_autonomos_seta_total_ue_y_no_ue', 8, 'numeric'),
            ('total_rg_y_reta_reg_e_autonomos_seta_union_europea', 9, 'numeric'),
            ('total_rg_y_reta_reg_e_autonomos_seta_no_union', 10, 'numeric'),
            ('total_rg_y_reta_regimen_general_total_ue_y_no_ue', 11, 'numeric'),
            ('total_rg_y_reta_regimen_general_union_europea', 12, 'numeric'),
            ('total_rg_y_reta_regimen_general_no_union', 13, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_total_regi_c86a757e', 14, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_total_regi_ac41c250', 15, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_total_regi_9f8af873', 16, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_36d1ef26', 17, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_41a81308', 18, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_r_e_autono_e4050695', 19, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_cbf9f456', 20, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_72b570f5', 21, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_reg_e_auto_13065814', 22, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_2b144925', 23, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_26f1c619', 24, 'numeric'),
            ('a_agricultura_ganadera_silvicultura_y_pesca_regimen_ge_a7624079', 25, 'numeric'),
            ('b_industrias_extractivas_total_regimen_total_ue_y_no_ue', 26, 'numeric'),
            ('b_industrias_extractivas_total_regimen_union_europea', 27, 'numeric'),
            ('b_industrias_extractivas_total_regimen_no_union', 28, 'numeric'),
            ('b_industrias_extractivas_r_e_autonomos_no_seta_total_ue_y_no_ue', 29, 'numeric'),
            ('b_industrias_extractivas_r_e_autonomos_no_seta_union_europea', 30, 'numeric'),
            ('b_industrias_extractivas_r_e_autonomos_no_seta_no_union', 31, 'numeric'),
            ('b_industrias_extractivas_reg_e_autonomos_seta_total_ue_y_no_ue', 32, 'numeric'),
            ('b_industrias_extractivas_reg_e_autonomos_seta_union_europea', 33, 'numeric'),
            ('b_industrias_extractivas_reg_e_autonomos_seta_no_union', 34, 'numeric'),
            ('b_industrias_extractivas_regimen_general_total_ue_y_no_ue', 35, 'numeric'),
            ('b_industrias_extractivas_regimen_general_union_europea', 36, 'numeric'),
            ('b_industrias_extractivas_regimen_general_no_union', 37, 'numeric'),
            ('c_industria_manufacturera_total_regimen_total_ue_y_no_ue', 38, 'numeric'),
            ('c_industria_manufacturera_total_regimen_union_europea', 39, 'numeric'),
            ('c_industria_manufacturera_total_regimen_no_union', 40, 'numeric'),
            ('c_industria_manufacturera_r_e_autonomos_no_seta_total_769b9e89', 41, 'numeric'),
            ('c_industria_manufacturera_r_e_autonomos_no_seta_union_europea', 42, 'numeric'),
            ('c_industria_manufacturera_r_e_autonomos_no_seta_no_union', 43, 'numeric'),
            ('c_industria_manufacturera_reg_e_autonomos_seta_total_ue_y_no_ue', 44, 'numeric'),
            ('c_industria_manufacturera_reg_e_autonomos_seta_union_europea', 45, 'numeric'),
            ('c_industria_manufacturera_reg_e_autonomos_seta_no_union', 46, 'numeric'),
            ('c_industria_manufacturera_regimen_general_total_ue_y_no_ue', 47, 'numeric'),
            ('c_industria_manufacturera_regimen_general_union_europea', 48, 'numeric'),
            ('c_industria_manufacturera_regimen_general_no_union', 49, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_66c9ca19', 50, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_8bc01880', 51, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_0503f5c1', 52, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_d9779513', 53, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_84138853', 54, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_13ef912b', 55, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_5b1f3cd7', 56, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_5cdd6b5d', 57, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_a6a7ed01', 58, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_aa7ab2c1', 59, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_792e9c60', 60, 'numeric'),
            ('d_suministro_de_energia_electrica_gas_vapor_y_aire_aco_7f8642a5', 61, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_2ae59acc', 62, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_348a6da9', 63, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_d5b2750f', 64, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_ef6ae68d', 65, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_8e704702', 66, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_23825dc3', 67, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_8ef7de79', 68, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_c78a2072', 69, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_c5a37613', 70, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_f60ed31e', 71, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_919aba74', 72, 'numeric'),
            ('e_suministro_de_agua_actividades_de_saneamiento_gestin_f11ebf12', 73, 'numeric'),
            ('f_construccin_total_regimen_total_ue_y_no_ue', 74, 'numeric'),
            ('f_construccin_total_regimen_union_europea', 75, 'numeric'),
            ('f_construccin_total_regimen_no_union', 76, 'numeric'),
            ('f_construccin_r_e_autonomos_no_seta_total_ue_y_no_ue', 77, 'numeric'),
            ('f_construccin_r_e_autonomos_no_seta_union_europea', 78, 'numeric'),
            ('f_construccin_r_e_autonomos_no_seta_no_union', 79, 'numeric'),
            ('f_construccin_reg_e_autonomos_seta_total_ue_y_no_ue', 80, 'numeric'),
            ('f_construccin_reg_e_autonomos_seta_union_europea', 81, 'numeric'),
            ('f_construccin_reg_e_autonomos_seta_no_union', 82, 'numeric'),
            ('f_construccin_regimen_general_total_ue_y_no_ue', 83, 'numeric'),
            ('f_construccin_regimen_general_union_europea', 84, 'numeric'),
            ('f_construccin_regimen_general_no_union', 85, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_total_regimen_t_a53d2a85', 86, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_total_regimen_u_1cc44bc3', 87, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_total_regimen_no_union', 88, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_7e830c82', 89, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_a7a41331', 90, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_r_e_autonomos_n_eb5d27f1', 91, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_c40ab3b8', 92, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_c073afa4', 93, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_reg_e_autonomos_1a844c57', 94, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_regimen_general_89f4b669', 95, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_regimen_general_eac4f084', 96, 'numeric'),
            ('g_comercio_al_por_mayor_y_al_por_menor_regimen_general_no_union', 97, 'numeric'),
            ('h_transporte_y_almacenamiento_total_regimen_total_ue_y_no_ue', 98, 'numeric'),
            ('h_transporte_y_almacenamiento_total_regimen_union_europea', 99, 'numeric'),
            ('h_transporte_y_almacenamiento_total_regimen_no_union', 100, 'numeric'),
            ('h_transporte_y_almacenamiento_r_e_autonomos_no_seta_to_6f7f0c8c', 101, 'numeric'),
            ('h_transporte_y_almacenamiento_r_e_autonomos_no_seta_un_40d870fc', 102, 'numeric'),
            ('h_transporte_y_almacenamiento_r_e_autonomos_no_seta_no_union', 103, 'numeric'),
            ('h_transporte_y_almacenamiento_reg_e_autonomos_seta_tot_aee1802b', 104, 'numeric'),
            ('h_transporte_y_almacenamiento_reg_e_autonomos_seta_uni_b2e33c27', 105, 'numeric'),
            ('h_transporte_y_almacenamiento_reg_e_autonomos_seta_no_union', 106, 'numeric'),
            ('h_transporte_y_almacenamiento_regimen_general_total_ue_y_no_ue', 107, 'numeric'),
            ('h_transporte_y_almacenamiento_regimen_general_union_europea', 108, 'numeric'),
            ('h_transporte_y_almacenamiento_regimen_general_no_union', 109, 'numeric'),
            ('i_hostelera_total_regimen_total_ue_y_no_ue', 110, 'numeric'),
            ('i_hostelera_total_regimen_union_europea', 111, 'numeric'),
            ('i_hostelera_total_regimen_no_union', 112, 'numeric'),
            ('i_hostelera_r_e_autonomos_no_seta_total_ue_y_no_ue', 113, 'numeric'),
            ('i_hostelera_r_e_autonomos_no_seta_union_europea', 114, 'numeric'),
            ('i_hostelera_r_e_autonomos_no_seta_no_union', 115, 'numeric'),
            ('i_hostelera_reg_e_autonomos_seta_total_ue_y_no_ue', 116, 'numeric'),
            ('i_hostelera_reg_e_autonomos_seta_union_europea', 117, 'numeric'),
            ('i_hostelera_reg_e_autonomos_seta_no_union', 118, 'numeric'),
            ('i_hostelera_regimen_general_total_ue_y_no_ue', 119, 'numeric'),
            ('i_hostelera_regimen_general_union_europea', 120, 'numeric'),
            ('i_hostelera_regimen_general_no_union', 121, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_d010762d', 122, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_5a74561b', 123, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_84cee81e', 124, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_3ba7f8a8', 125, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_15d135ea', 126, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_9da5469e', 127, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_4fbe8fa4', 128, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_106dc4a7', 129, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_2542c800', 130, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_f15b851d', 131, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_a26b3378', 132, 'numeric'),
            ('j_actividades_de_edicin_radiodifusin_y_produccin_y_dis_5a36de72', 133, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_d0982e63', 134, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_e800632f', 135, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_dde7652b', 136, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_213dc215', 137, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_6bcebcbb', 138, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_511cbaff', 139, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_bf1d18e0', 140, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_a9939f17', 141, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_d1c4ba9f', 142, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_1a4e4a6d', 143, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_f505c98b', 144, 'numeric'),
            ('k_telecomunicaciones_programacin_informtica_consultora_73b9b58c', 145, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_total_regimen_t_ab32213f', 146, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_total_regimen_u_d6039741', 147, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_total_regimen_no_union', 148, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_r_e_autonomos_n_0a0e7ae8', 149, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_r_e_autonomos_n_21d682d3', 150, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_r_e_autonomos_n_baa86b30', 151, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_reg_e_autonomos_93c7dc34', 152, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_reg_e_autonomos_c9db9737', 153, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_reg_e_autonomos_73c72e78', 154, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_regimen_general_d1a398e5', 155, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_regimen_general_2a18de0f', 156, 'numeric'),
            ('l_actividades_financieras_y_de_seguros_regimen_general_no_union', 157, 'numeric'),
            ('m_actividades_inmobiliarias_total_regimen_total_ue_y_no_ue', 158, 'numeric'),
            ('m_actividades_inmobiliarias_total_regimen_union_europea', 159, 'numeric'),
            ('m_actividades_inmobiliarias_total_regimen_no_union', 160, 'numeric'),
            ('m_actividades_inmobiliarias_r_e_autonomos_no_seta_tota_5cbb832b', 161, 'numeric'),
            ('m_actividades_inmobiliarias_r_e_autonomos_no_seta_union_europea', 162, 'numeric'),
            ('m_actividades_inmobiliarias_r_e_autonomos_no_seta_no_union', 163, 'numeric'),
            ('m_actividades_inmobiliarias_reg_e_autonomos_seta_total_3784bbb0', 164, 'numeric'),
            ('m_actividades_inmobiliarias_reg_e_autonomos_seta_union_europea', 165, 'numeric'),
            ('m_actividades_inmobiliarias_reg_e_autonomos_seta_no_union', 166, 'numeric'),
            ('m_actividades_inmobiliarias_regimen_general_total_ue_y_no_ue', 167, 'numeric'),
            ('m_actividades_inmobiliarias_regimen_general_union_europea', 168, 'numeric'),
            ('m_actividades_inmobiliarias_regimen_general_no_union', 169, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_total_40f76b93', 170, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_total_f456bb5c', 171, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_total_bab0bb75', 172, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_89cda3e6', 173, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_f33bfaab', 174, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_r_e_a_f30651cf', 175, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_reg_e_d0732312', 176, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_reg_e_f37abe62', 177, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_reg_e_7bc9bbdc', 178, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_regim_1395566a', 179, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_regim_4461700f', 180, 'numeric'),
            ('n_actividades_profesionales_cientficas_y_tcnicas_regim_6ea486de', 181, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_t_4e04f725', 182, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_t_63da1a6d', 183, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_t_b4b8a5c0', 184, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_b3721462', 185, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_2af015d5', 186, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_1f672487', 187, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_282f0372', 188, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_c9d42787', 189, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_c544a449', 190, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_bd2cba12', 191, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_50696672', 192, 'numeric'),
            ('o_actividades_administrativas_y_servicios_auxiliares_r_ed444dd1', 193, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_0177e7d8', 194, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_13f6b177', 195, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_d4197c40', 196, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_e83e6b57', 197, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_13e98690', 198, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_d9834483', 199, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_8ededd53', 200, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_42ab8fbd', 201, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_77c86968', 202, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_8ab0c373', 203, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_b89cabb5', 204, 'numeric'),
            ('p_administracin_pblica_y_defensa_seguridad_social_obli_8a45decf', 205, 'numeric'),
            ('q_educacin_total_regimen_total_ue_y_no_ue', 206, 'numeric'),
            ('q_educacin_total_regimen_union_europea', 207, 'numeric'),
            ('q_educacin_total_regimen_no_union', 208, 'numeric'),
            ('q_educacin_r_e_autonomos_no_seta_total_ue_y_no_ue', 209, 'numeric'),
            ('q_educacin_r_e_autonomos_no_seta_union_europea', 210, 'numeric'),
            ('q_educacin_r_e_autonomos_no_seta_no_union', 211, 'numeric'),
            ('q_educacin_reg_e_autonomos_seta_total_ue_y_no_ue', 212, 'numeric'),
            ('q_educacin_reg_e_autonomos_seta_union_europea', 213, 'numeric'),
            ('q_educacin_reg_e_autonomos_seta_no_union', 214, 'numeric'),
            ('q_educacin_regimen_general_total_ue_y_no_ue', 215, 'numeric'),
            ('q_educacin_regimen_general_union_europea', 216, 'numeric'),
            ('q_educacin_regimen_general_no_union', 217, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_total_29aa544b', 218, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_total_efc74c90', 219, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_total_2fc6dd19', 220, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_5194529b', 221, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_9f38e60d', 222, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_r_e_a_a0a13fef', 223, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_reg_e_af894175', 224, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_reg_e_a1cbde0d', 225, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_reg_e_530cf441', 226, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_regim_1c87e25a', 227, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_regim_e6b28bc7', 228, 'numeric'),
            ('r_actividades_sanitarias_y_de_servicios_sociales_regim_a017ac95', 229, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_13f6a7e6', 230, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_cc1eaeb5', 231, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_00ec14b5', 232, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_1c7119c9', 233, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_e456269a', 234, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_4a01afcc', 235, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_f07e8451', 236, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_573e2370', 237, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_dbfda443', 238, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_464aad44', 239, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_ee4681e2', 240, 'numeric'),
            ('s_actividades_artsticas_deportivas_y_de_entretenimient_5c535f3e', 241, 'numeric'),
            ('t_otros_servicios_total_regimen_total_ue_y_no_ue', 242, 'numeric'),
            ('t_otros_servicios_total_regimen_union_europea', 243, 'numeric'),
            ('t_otros_servicios_total_regimen_no_union', 244, 'numeric'),
            ('t_otros_servicios_r_e_autonomos_no_seta_total_ue_y_no_ue', 245, 'numeric'),
            ('t_otros_servicios_r_e_autonomos_no_seta_union_europea', 246, 'numeric'),
            ('t_otros_servicios_r_e_autonomos_no_seta_no_union', 247, 'numeric'),
            ('t_otros_servicios_reg_e_autonomos_seta_total_ue_y_no_ue', 248, 'numeric'),
            ('t_otros_servicios_reg_e_autonomos_seta_union_europea', 249, 'numeric'),
            ('t_otros_servicios_reg_e_autonomos_seta_no_union', 250, 'numeric'),
            ('t_otros_servicios_regimen_general_total_ue_y_no_ue', 251, 'numeric'),
            ('t_otros_servicios_regimen_general_union_europea', 252, 'numeric'),
            ('t_otros_servicios_regimen_general_no_union', 253, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_bc7e5ec9', 254, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_3d7e96e1', 255, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_6283f11f', 256, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_07fe8f94', 257, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_28246b88', 258, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_c1548fa3', 259, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_e78bc9f6', 260, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_ac0cfca5', 261, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_84b380d3', 262, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_a8b5e8dc', 263, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_47c96fc9', 264, 'numeric'),
            ('u_actividades_de_los_hogares_como_empleadores_de_perso_fab1e846', 265, 'numeric'),
            ('v_organismos_extraterritoriales_total_regimen_total_ue_y_no_ue', 266, 'numeric'),
            ('v_organismos_extraterritoriales_total_regimen_union_europea', 267, 'numeric'),
            ('v_organismos_extraterritoriales_total_regimen_no_union', 268, 'numeric'),
            ('v_organismos_extraterritoriales_r_e_autonomos_no_seta_fe750fe4', 269, 'numeric'),
            ('v_organismos_extraterritoriales_r_e_autonomos_no_seta_06224da8', 270, 'numeric'),
            ('v_organismos_extraterritoriales_r_e_autonomos_no_seta_no_union', 271, 'numeric'),
            ('v_organismos_extraterritoriales_reg_e_autonomos_seta_t_f1f68ea3', 272, 'numeric'),
            ('v_organismos_extraterritoriales_reg_e_autonomos_seta_u_188fa123', 273, 'numeric'),
            ('v_organismos_extraterritoriales_reg_e_autonomos_seta_no_union', 274, 'numeric'),
            ('v_organismos_extraterritoriales_regimen_general_total_0de9cba8', 275, 'numeric'),
            ('v_organismos_extraterritoriales_regimen_general_union_europea', 276, 'numeric'),
            ('v_organismos_extraterritoriales_regimen_general_no_union', 277, 'numeric'),
        ],
    },
    {
        "dataset_name": 'tech_salaries',
        "file_name": '(Datos Externos) Salarios Tecnológicos - Kaggle.csv',
        "table_name": 'src.tech_salaries_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ',',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'annual',
        "time_field": 'work_year',
        "series_keys": ['experience_level', 'employment_type', 'job_title', 'salary_currency', 'employee_residence', 'company_location', 'company_size'],
        "value_fields": ['salary', 'salary_in_usd', 'remote_ratio'],
        "columns": [
            ('work_year', 0, 'integer'),
            ('experience_level', 1, 'text'),
            ('employment_type', 2, 'text'),
            ('job_title', 3, 'text'),
            ('salary', 4, 'integer'),
            ('salary_currency', 5, 'text'),
            ('salary_in_usd', 6, 'integer'),
            ('employee_residence', 7, 'text'),
            ('remote_ratio', 8, 'integer'),
            ('company_location', 9, 'text'),
            ('company_size', 10, 'text'),
        ],
    },
    {
        "dataset_name": 'emigration_pere',
        "file_name": '(Datos Externos) Datos de Emigración - INE.csv',
        "table_name": 'src.emigration_pere_raw',
        "encoding": 'utf-8-sig',
        "delimiter": ';',
        "header_rows": 1,
        "decimal_style": 'dot',
        "temporal_type": 'annual',
        "time_field": 'periodo',
        "series_keys": ['pais_de_residencia', 'total_nacional', 'comunidades_y_ciudades_autonomas', 'provincias', 'sexo', 'lugar_de_nacimiento'],
        "value_fields": [],
        "columns": [
            ('pais_de_residencia', 0, 'text'),
            ('total_nacional', 1, 'text'),
            ('comunidades_y_ciudades_autonomas', 2, 'text'),
            ('provincias', 3, 'text'),
            ('sexo', 4, 'text'),
            ('lugar_de_nacimiento', 5, 'text'),
            ('periodo', 6, 'integer'),
            ('total', 7, 'text'),
        ],
    },
]


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_git_lfs_pointer(path):
    """Detecta punteros de Git LFS que apuntan a un fichero no descargado."""
    with path.open("rb") as handle:
        return handle.read(64).startswith(b"version https://git-lfs")


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


def convert_value(raw_value, column_type, decimal_style):
    """Convierte un valor textual del CSV al tipo SQL de la columna.

    Los valores vacios de columnas no textuales se insertan como NULL. Las
    columnas TEXT conservan el valor original, incluidos los espacios y las
    cadenas vacias.
    """
    if column_type == "text":
        return raw_value

    value = raw_value.strip()
    if value == "":
        return None

    if column_type == "integer":
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"valor entero no valido: {raw_value!r}") from exc

    if column_type == "numeric":
        normalized = value
        if decimal_style == "comma":
            # Formato espanol: 1.234,56 -> 1234.56
            normalized = normalized.replace(".", "").replace(",", ".")
        try:
            return Decimal(normalized)
        except InvalidOperation as exc:
            raise ValueError(f"valor numerico no valido: {raw_value!r}") from exc

    if column_type == "date":
        # No hay columnas DATE en el catalogo actual; se deja preparado para
        # futuras fuentes. PostgreSQL castea la cadena ISO al tipo DATE.
        return value

    raise ValueError(f"tipo de columna no soportado: {column_type!r}")


def build_row_values(row, columns, decimal_style):
    values = []
    for name, index, column_type in columns:
        if index >= len(row):
            raise ValueError(
                f"falta la columna {name!r} (indice {index}); "
                f"la fila solo tiene {len(row)} campos"
            )
        values.append(convert_value(row[index], column_type, decimal_style))
    return values


def log_failure(conn, load_id, dataset, file_hash, started_at, error):
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
                    dataset["dataset_name"],
                    dataset["file_name"],
                    file_hash,
                    started_at,
                    finished_at,
                    str(error)[:2000],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()


def load_rows(conn, dataset, file_path, file_hash, existing_hash, force, load_id, started_at):
    dataset_name = dataset["dataset_name"]
    table_name = dataset["table_name"]
    columns = dataset["columns"]
    header_rows = dataset["header_rows"]
    decimal_style = dataset["decimal_style"]
    expected_columns = max(index for _, index, _ in columns) + 1
    data_start_row = header_rows + 1

    if is_git_lfs_pointer(file_path):
        raise RuntimeError(
            "el fichero es un puntero de Git LFS, no un CSV real. "
            "Ejecuta 'git lfs pull' para descargar su contenido antes de cargarlo."
        )

    header_lines = []
    row_count = 0

    with file_path.open("r", encoding=dataset["encoding"], newline="") as handle:
        reader = csv.reader(handle, delimiter=dataset["delimiter"])

        for _ in range(header_rows):
            try:
                header_lines.append(next(reader))
            except StopIteration as exc:
                raise RuntimeError(
                    f"el fichero no contiene las {header_rows} filas de "
                    "cabecera esperadas"
                ) from exc

        column_names = [name for name, _, _ in columns]
        quoted_columns = ", ".join(f'"{name}"' for name in column_names)
        insert_sql = (
            f"INSERT INTO {table_name} "
            f"(load_id, row_number, {quoted_columns}, record_origin) VALUES %s"
        )
        params_per_row = len(column_names) + 3  # load_id + row_number + record_origin
        page_size = max(1, min(BATCH_SIZE, MAX_BIND_PARAMS // params_per_row))

        with conn.cursor() as cur:
            if force and existing_hash is not None:
                cur.execute(f"DELETE FROM {table_name} WHERE record_origin = 'real'")
                cur.execute(
                    "DELETE FROM src.dataset_files WHERE dataset_name = %s",
                    (dataset_name,),
                )

            batch = []
            row_number = data_start_row

            for row in reader:
                if len(row) != expected_columns:
                    raise ValueError(
                        f"la fila {row_number} tiene {len(row)} columnas; "
                        f"se esperaban {expected_columns}"
                    )

                values = build_row_values(row, columns, decimal_style)
                batch.append((load_id, row_number, *values, "real"))
                row_count += 1
                row_number += 1

                if len(batch) >= page_size:
                    execute_values(cur, insert_sql, batch, page_size=page_size)
                    batch.clear()
                    if row_count % 100000 == 0:
                        print(f"  {dataset_name}: {row_count:,} filas")

            if batch:
                execute_values(cur, insert_sql, batch, page_size=page_size)

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
                    Json(dataset["value_fields"]),
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

    return row_count


def load_dataset(conn, dataset, force=False):
    dataset_name = dataset["dataset_name"]
    file_path = DATA_SOURCES_DIR / dataset["file_name"]

    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el fichero: {file_path}")

    file_hash = sha256_file(file_path)

    with conn.cursor() as cur:
        existing_hash = get_existing_hash(cur, dataset_name)

    if existing_hash == file_hash and not force:
        print(f"[SKIP] {dataset_name}: ya cargado con el mismo SHA-256")
        return 0

    if existing_hash is not None and existing_hash != file_hash and not force:
        raise RuntimeError(
            f"{dataset_name}: el fichero ha cambiado. Usa --force para recargarlo."
        )

    load_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)

    try:
        row_count = load_rows(
            conn,
            dataset,
            file_path,
            file_hash,
            existing_hash,
            force,
            load_id,
            started_at,
        )
        conn.commit()
        print(f"[OK] {dataset_name}: {row_count:,} filas")
        return row_count
    except Exception as exc:
        conn.rollback()
        log_failure(conn, load_id, dataset, file_hash, started_at, exc)
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Carga los CSV de origen en las tablas Raw de PostgreSQL."
    )
    parser.add_argument(
        "--dataset",
        choices=[dataset["dataset_name"] for dataset in DATASETS_CATALOG],
        help="Carga solo el dataset indicado.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recarga el dataset aunque el SHA-256 ya este registrado.",
    )
    args = parser.parse_args()

    selected = [
        dataset
        for dataset in DATASETS_CATALOG
        if args.dataset in (None, dataset["dataset_name"])
    ]

    conn = get_connection()
    failures = []
    try:
        for dataset in selected:
            try:
                load_dataset(conn, dataset, force=args.force)
            except Exception as exc:
                failures.append((dataset["dataset_name"], exc))
                print(
                    f"[ERROR] {dataset['dataset_name']}: {exc}",
                    file=sys.stderr,
                )
    finally:
        conn.close()

    if failures:
        print("\nResumen de errores:", file=sys.stderr)
        for dataset_name, exc in failures:
            print(f"  - {dataset_name}: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
