# Fase 1: Sistema Origen y Carga Raw (PostgreSQL 1)

## 📌 Propósito de la Carpeta

Este directorio contiene todo lo necesario para desplegar el contenedor PostgreSQL que actúa como **sistema origen** del proyecto:

1. **Alojar las 10 fuentes CSV oficiales** descargadas del INE, Eurostat, ISTAC y TGSS.
2. **Cargarlas en el esquema `src`** en tablas Raw con **columnas físicas explícitas**, nombres normalizados en `snake_case` y tipos SQL nativos (`TEXT`, `INTEGER`, `NUMERIC`, `DATE`).
3. **Registrar la trazabilidad** de cada carga en el esquema `audit` y en `src.dataset_files`.
4. Servir de base para la **proyección temporal sintética con SDV** (Fase 1, anexo) y para la extracción incremental de Airbyte (Fase 2).

> **Cambio arquitectónico:** las tablas Raw **ya no utilizan `payload JSONB`**. Cada fila del CSV se desempaqueta directamente en las columnas físicas de su tabla destino.

---

## 📂 Estructura de Directorios Interna

```text
01_source_system/
├── Dockerfile                       # Imagen PostgreSQL 16 con los scripts de init
├── README.md
├── PLAN_IMPLEMENTACION_FASE_1.md
├── init-scripts/                    # Scripts ejecutados al inicializar la BD
│   ├── 00_bootstrap_roles.sh        # Roles src_writer y src_reader
│   ├── 01_create_schemas.sql        # Esquemas src y audit
│   ├── 02_create_tables.sql         # DDL de tablas Raw con columnas físicas
│   ├── 03_indexes_constraints.sql   # Índices y unicidad
│   └── 04_grants.sql                # Permisos por rol
├── loaders/
│   ├── load_sources_raw.py          # Carga idempotente de los 10 CSV
│   └── requirements.txt             # psycopg2-binary, python-dotenv
├── synthetic_data/                  # Proyección temporal SDV (pendiente)
│   ├── config.yaml
│   ├── prepare_own_data.py
│   ├── simulate_future_raw_data.py
│   ├── evaluate_synthetic.py
│   └── models/
└── data_sources/                    # 10 CSV originales (inmutables)
```

---

## 📊 Fuentes de Datos Referenciadas

Todas las rutas son relativas a `01_source_system/data_sources/`. Los nombres de fichero se muestran **exactamente** como están en el repositorio (incluyen espacios y paréntesis), por lo que deben citarse entre comillas en scripts SQL/Python.

| Fichero exacto en el repositorio | Fuente / Organismo | Enlace de acceso / descarga | Formato y particularidades | Granularidad | Motivo principal / KPI |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | Kaggle (Numbeo) | [Global Cost of Living](https://www.kaggle.com/datasets/mvieira101/global-cost-of-living) | CSV ASCII, separador `,`, ~4.956 filas, columnas genéricas `x1..x55` (requieren mapeo a campos Numbeo) | Ciudad / país | **Ratio Salario / Coste de Vida** e impacto inmobiliario |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social.csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | CSV **ISO-8859-1**, separador `;`, CRLF, ~3.540 filas, formato ancho | Periodo × CCAA × régimen × sexo × UE/no UE | Demografía y volumen bruto de trabajadores extranjeros |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | Eurostat | [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/view/hrst_st_nocc/default/table) | CSV ASCII, separador `,`, ~66.088 filas, formato largo con flags de observación | Año × país × ocupación ISCO-08 × edad | Movilidad de perfiles de alta tecnología, menores de 35 años |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | INE | [EPA - INE](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176918&menu=resultados&idp=1254735976595) | UTF-8, separador `,`, cabecera multinivel, **coma decimal** | Sexo × ocupados/parados/tasas | Referencia oficial de ocupación y paro en perfiles cualificados |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, cabecera multinivel, ~990 filas | Trimestre × CNAE-25 × cuenta ajena/propia | Puestos cubiertos en sectores técnicos e ingeniería en Canarias |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, formato ancho, 1999–2008 | Trimestre × división de actividad | Series históricas de empleo desglosadas por divisiones |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, formato ancho, 1999–2026 T2 | Trimestre × situación laboral | Evaluación de asalariados y autónomos en Canarias |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | UTF-8, separador `;`, CRLF, ~180 filas, columnas muy largas | Periodo × CCAA × sección CNAE × régimen × UE/no UE | Aislar el sector TIC (CNAE Sección J/K) para talento tecnológico |
| `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` | Kaggle (Adil Shamim) | [Data Science, AI & ML Job Salaries in 2025](https://www.kaggle.com/datasets/adilshamim8/salaries-for-data-science-jobs) | UTF-8 con BOM (`utf-8-sig`), separador `,`, LF, ~151.445 filas, formato largo (una fila por salario) | Año × nivel de experiencia × tipo de empleo × puesto × país de residencia/empresa × modalidad remota | Benchmark salarial de perfiles tecnológicos; brecha salarial y ratio salario/coste de vida |
| `(Datos Externos) Datos de Emigración - INE.csv` | INE | [PERE - Población Española Residente en el Extranjero (tabla 79678)](https://www.ine.es/jaxiT3/Tabla.htm?L=0&t=79678) | UTF-8 con BOM (`utf-8-sig`), separador `;`, CRLF, ~4.043.520 filas, formato largo, totales con separador de miles y celdas vacías `""` | Año × país de residencia / CCAA / provincia × sexo × lugar de nacimiento | Stock de españoles residentes en el extranjero; proxy de emigración/fuga de talento y saldo neto |

> El fichero de emigración del INE está versionado con **Git LFS**. Si en el clon local aparece como un fichero de texto de tres líneas (`version https://git-lfs...`), ejecuta `git lfs pull` antes de cargarlo.

---

## 🗄️ Modelo Raw: columnas físicas y auditoría

Cada una de las 10 tablas `src.<dataset>_raw` contiene:

- Una columna física por cada columna útil del CSV, con nombre normalizado `snake_case`.
- Tipos SQL nativos: `TEXT`, `INTEGER`, `NUMERIC` y `DATE` (este último reservado para futuras fuentes).
- Las columnas de auditoría y trazabilidad comunes:

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `row_id` | `BIGSERIAL` PK | Identificador monotónico; cursor incremental para Airbyte |
| `load_id` | `UUID NOT NULL` | Ejecución de carga que insertó la fila |
| `row_number` | `INTEGER NOT NULL` | Número de fila física del CSV (1-based, incluidas cabeceras) |
| `record_origin` | `VARCHAR(20) DEFAULT 'real'` | `real` o `synthetic` |
| `synthetic_run_id` | `UUID` | Ejecución de proyección sintética (NULL en filas reales) |
| `synthetic_period` | `VARCHAR(30)` | Periodo futuro proyectado (NULL en filas reales) |
| `loaded_at` | `TIMESTAMPTZ DEFAULT NOW()` | Marca temporal de inserción |

### Tablas y número de columnas físicas

| Tabla Raw | Fichero origen | Columnas físicas | Claves de lectura / dimensión | Campos numéricos |
| :--- | :--- | ---: | :--- | :--- |
| `src.cost_of_living_raw` | `(Datos Externos)  costo de vida global -  Kaggle.csv` | 58 | `city`, `country` | `x1..x55`, `data_quality` |
| `src.foreign_affiliates_gender_raw` | `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social.csv` | 110 | `periodo`, `ccaa` | 108 medidas TGSS |
| `src.eurostat_hitech_employment_raw` | `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | 23 | `time_period`, `freq`, `category`, `isco08`, `age`, `unit`, `geo` | `obs_value` |
| `src.epa_labor_market_raw` | `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | 9 | `region` | 8 medidas (ocupados, parados y tasas por sexo) |
| `src.istac_employment_cnae_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | 257 | `geography`, `activity` | 255 medidas periodo × tipo de empleo |
| `src.istac_employment_divisions_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | 41 | `activity` | 40 periodos trimestrales |
| `src.istac_employment_status_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | 139 | `geography`, `employment_status` | 137 periodos trimestrales |
| `src.foreign_affiliates_cnae_raw` | `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | 278 | `periodo`, `ccaa` | 276 medidas TGSS |
| `src.tech_salaries_raw` | `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` | 11 | `work_year`, `experience_level`, `employment_type`, `job_title`, `employee_residence`, `company_location`, `company_size` | `salary`, `salary_in_usd`, `remote_ratio` |
| `src.emigration_pere_raw` | `(Datos Externos) Datos de Emigración - INE.csv` | 8 | `periodo`, `pais_de_residencia`, `total_nacional`, `comunidades_y_ciudades_autonomas`, `provincias`, `sexo`, `lugar_de_nacimiento` | `total` (TEXT, conserva separador de miles) |

### Criterios de normalización de nombres

- Se eliminan acentos, se pasa a minúsculas y se sustituyen caracteres no alfanuméricos por `_`.
- Las cabeceras multinivel se aplanan combinando periodo y medida. Ejemplo:
  `Empleos por cuenta ajena` + `2025 Cuarto trimestre` → `empleos_por_cuenta_ajena_2025_cuarto_trimestre`.
- Los datasets anchos (ISTAC y TGSS) conservan una columna física por cada celda del CSV; no se hace `melt` en la capa Raw.
- PostgreSQL limita los identificadores a 63 bytes. Los nombres que superan ese límite se truncan de forma determinista y se les añade un sufijo hash de 8 caracteres para garantizar unicidad.
- Las cabeceras originales completas se conservan en `src.dataset_files.header_lines`.

---

## 🔗 Trazabilidad en el Pipeline

```text
01_source_system/data_sources/*.csv
        │  (carga inicial - Fase 1, columnas físicas tipadas)
        ▼
PostgreSQL 1 (src.* + audit.*)
        │  (Airbyte EL - Fase 2, cursor row_id/loaded_at)
        ▼
03_raw_database (Landing Zone)
        │  (dbt: staging → intermediate → marts - Fase 4)
        ▼
Data Marts / KPIs
```

---

## 🧩 Matriz Fuente → KPI

| KPI | Fuentes implicadas |
| :--- | :--- |
| Tasa de Saldo Neto de Talento | TGSS (`3mb...` y `5mb25...`), EPA, ISTAC, PERE (`Datos de Emigración - INE`) |
| Ratio Salario / Coste de Vida | Kaggle (Numbeo) + Salarios Tecnológicos (Kaggle) |
| Absorción de Perfiles Junior | Eurostat (alta tecnología), EPA, ISTAC CNAE-25 |

---

## ⚠️ Notas Técnicas de Ingestión

- **Sin `payload JSONB`:** cada fila del CSV se inserta directamente en columnas físicas tipadas.
- **Cabeceras:** el loader ignora las filas de cabecera indicadas por `header_rows` y las guarda íntegras en `src.dataset_files.header_lines`.
- **Codificaciones:** `3mb...` se lee como `latin-1`; el resto como `utf-8-sig` (elimina el BOM).
- **Separadores:** `;` para TGSS y PERE; `,` para el resto.
- **Cabeceras multinivel:** EPA, ISTAC CNAE25 y TGSS 5mb25 tienen 2 filas de cabecera; `3mb...` tiene título + línea vacía + cabecera (3 filas). ISTAC status y divisiones tienen 1 fila de cabecera.
- **Coma decimal:** el loader convierte los valores EPA (`1.234,56`) a `NUMERIC` (`1234.56`). El resto de medidas numéricas usan punto decimal.
- **Celdas vacías:** en columnas `INTEGER`/`NUMERIC` se insertan como `NULL`; en columnas `TEXT` se conserva la cadena original.
- **PERE:** `total` se mantiene como `TEXT` para conservar el separador de miles y las celdas `""`; `periodo` se tipa como `INTEGER`.
- **Salarios:** se conservan `salary` y `salary_currency` originales; `salary_in_usd` queda disponible para comparaciones homogéneas en dbt.
- **Ficheros inmutables:** el loader abre los CSV en modo lectura y nunca escribe en `data_sources/`.
- **Git LFS:** el fichero de emigración del INE requiere `git lfs pull` si no se ha descargado el contenido real.
- Los nombres de fichero contienen espacios (incluidos dobles), paréntesis y espacios antes de la extensión; se referencian entre comillas en los scripts.

---

## 🛠️ Pasos de Implementación

1. **Levantar PostgreSQL 1:**
   ```bash
   docker compose up -d source_db
   docker compose ps
   ```

2. **Cargar los CSV en columnas físicas:**
   ```bash
   python 01_source_system/loaders/load_sources_raw.py
   ```
   Para recargar un dataset concreto:
   ```bash
   python 01_source_system/loaders/load_sources_raw.py --dataset tech_salaries --force
   ```

3. **Verificar la carga:**
   ```sql
   SELECT dataset_name, row_count, sha256, loaded_at
   FROM src.dataset_files
   ORDER BY dataset_name;

   SELECT dataset_name, rows_loaded, status, started_at
   FROM audit.load_log
   ORDER BY started_at DESC;
   ```

4. **Proyección temporal sintética (SDV, pendiente):**
   ```bash
   python 01_source_system/synthetic_data/prepare_own_data.py
   python 01_source_system/synthetic_data/simulate_future_raw_data.py --all
   ```

5. **Conexión con Airbyte:** este contenedor se configurará como base de datos de **origen** en la Fase 2. El cursor incremental recomendado es `row_id` (monotónico) y `loaded_at` como marca de control.

---

## 🧯 Troubleshooting

- **Reinicializar desde cero** (borra el volumen y vuelve a ejecutar los scripts de init):
  ```bash
  docker compose down -v
  docker compose up -d source_db
  ```
- **Reejecutar una carga:** usa `--force` para borrar las filas reales del dataset y volver a insertarlas. Las filas sintéticas no se eliminan.
- **El loader se salta un dataset:** el SHA-256 ya está registrado en `src.dataset_files`. Usa `--force` si has cambiado el fichero.
- **Error de Git LFS en PERE:** ejecuta `git lfs pull` y vuelve a lanzar el loader.
- **Los scripts de init solo se ejecutan con el volumen vacío:** si cambias el DDL, reinicializa con `docker compose down -v`.

---

## 🔐 Handoff a la Fase 2

- **Esquemas a replicar:** `src` y `audit`.
- **Tablas:** 10 tablas Raw + `src.dataset_files` + `audit.load_log`.
- **Cursor incremental:** `row_id` y `loaded_at`.
- **Rol recomendado para Airbyte:** `src_reader` (solo lectura).
- **Distinción real/sintético:** `record_origin = 'real' | 'synthetic'`, `synthetic_run_id`, `synthetic_period`.
