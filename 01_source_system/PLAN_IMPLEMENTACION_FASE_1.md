# Plan de Implementación — Fase 1: Source System, Carga Raw y Simulación Temporal con SDV

> **Proyecto:** Gestión de Fuga y Atracción de Nómadas Digitales Cualificados en Canarias y España
> **Fase:** 1 de 4 — `01_source_system`
> **Componente:** PostgreSQL 1 (sistema origen)
> **Estado:** Parcialmente implementado (v4 — capa Raw con columnas físicas, sin JSONB)
> **Última actualización:** 2026-11-11

---

## 0. Principio rector

Esta fase construye **exclusivamente el sistema origen y su evolución temporal simulada**:

- Los 10 ficheros de `data_sources/` son **inmutables**: no se editan, recortan, renombran ni sobrescriben.
- Los CSV se cargan en el esquema `src.*` en **columnas físicas explícitas** (`snake_case` y tipos SQL nativos), preservando las filas y las cabeceras originales en `src.dataset_files.header_lines`.
- **No se utiliza `payload JSONB`** en las tablas Raw. Cada fila del CSV se desempaqueta directamente en las columnas de su tabla destino.
- **No se crean tablas nuevas para datos sintéticos.** SDV aprende de las tablas Raw existentes y añade registros sintéticos a esas mismas tablas.
- **No se simulan tablas transaccionales ficticias.** No existen esquemas ni entidades de tipo operacional.
- La generación sintética es un **anexo temporal**: añade periodos futuros a las series históricas para que Airbyte pueda capturar incrementales en la Fase 2.
- La limpieza semántica, el modelado de negocio y la normalización de categorías se realizarán en la **Fase 4 (dbt)**. El tipado físico y la conversión de formato (coma decimal, celdas vacías) se resuelven en la carga Raw.

---

## 1. Objetivo de la fase

Levantar el contenedor PostgreSQL del sistema origen, cargar los 10 ficheros CSV macroeconómicos reales en el esquema `src.*` con **columnas físicas tipadas**, y aplicar modelos de **SDV (Synthetic Data Vault)** sobre estas tablas para generar registros sintéticos futuros que simulen la evolución temporal agregada de las series.

Al finalizar la fase:

- El sistema origen estará operativo y persistente.
- Los CSV reales estarán cargados en columnas físicas con nombres normalizados y tipos SQL nativos.
- Las tablas Raw contendrán tanto registros reales como registros sintéticos futuros, claramente identificables.
- Airbyte podrá conectarse como lector y capturar los nuevos registros mediante un cursor incremental.

---

## 2. Alcance

### Incluido

- Renombrar la carpeta de la fase a `01_source_system/`.
- Creación del contenedor PostgreSQL 1 y su volumen persistente.
- Esquema `src` para la carga Raw de los CSV.
- Esquema `audit` para trazabilidad de cargas y proyecciones.
- Carga íntegra de los 10 CSV con mapeo explícito a columnas físicas tipadas.
- Extracción en memoria de las series temporales para entrenamiento SDV.
- Metadatos SDV y configuración de proyección temporal.
- Entrenamiento de `PARSynthesizer` para series temporales y de modelos tabulares para datos estáticos.
- Inserción de registros sintéticos futuros en las mismas tablas Raw.
- Roles `src_admin`, `src_writer` y `src_reader`.
- Validaciones de fidelidad, integridad y continuidad temporal.
- Documentación y handoff a la Fase 2.

### No incluido

- Simulación de tablas transaccionales ficticias.
- Creación de esquemas o tablas nuevas para los datos sintéticos.
- Limpieza semántica, normalización de categorías o modelado de negocio (se hará en dbt).
- Configuración de conectores Airbyte (Fase 2).
- Creación de la base de datos Raw de destino (Fase 3).
- Modelos dbt y cálculo de indicadores (Fase 4).
- Cuadros de mando o visualización.

---

## 3. Estado actual del repositorio

A fecha de este plan:

```text
01_source_system/
├── README.md
├── PLAN_IMPLEMENTACION_FASE_1.md
├── Dockerfile
├── data_sources/          # 10 CSV reales ya descargados (PERE requiere git lfs pull)
├── init-scripts/          # 00_bootstrap_roles.sh + 01..04 SQL implementados
├── loaders/
│   ├── load_sources_raw.py
│   ├── complete_export_dimensions.py
│   └── requirements.txt
└── synthetic_data/        # pendiente de implementación SDV
```

El servicio `source_db` ya está declarado en `docker-compose.yml`. La capa Raw está implementada con columnas físicas tipadas y sin `payload JSONB`. Las particularidades técnicas de los CSV están documentadas en el `README.md` de la fase.

---

## 4. Resultado esperado al finalizar la fase

```text
01_source_system/
├── Dockerfile
├── .env.example
├── README.md
├── PLAN_IMPLEMENTACION_FASE_1.md
├── init-scripts/
│   ├── 00_bootstrap_roles.sh
│   ├── 01_create_schemas.sql
│   ├── 02_create_tables.sql
│   ├── 03_indexes_constraints.sql
│   └── 04_grants.sql
├── loaders/
│   ├── load_sources_raw.py
│   ├── complete_export_dimensions.py
│   └── requirements.txt
├── synthetic_data/
│   ├── metadata.json
│   ├── config.yaml
│   ├── prepare_own_data.py
│   ├── simulate_future_raw_data.py
│   ├── evaluate_synthetic.py
│   ├── models/              # artefactos SDV entrenados
│   └── requirements.txt
└── data_sources/
    └── (10 CSV de referencia, sin modificar)
```

Además, en la raíz del repositorio se añadirá el servicio `source_db` dentro de `docker-compose.yml`, dejando preparada la incorporación futura de Airbyte y PostgreSQL 2.

---

## 5. Decisiones de diseño

| Decisión | Justificación |
| :--- | :--- |
| Renombrar la fase a `01_source_system` | La fase ya no simula un sistema transaccional; es un sistema origen de datos macroeconómicos. |
| No crear tablas nuevas para datos sintéticos | SDV debe aprender de las series Raw existentes y añadir registros a las mismas tablas. |
| Cargar los CSV en columnas físicas explícitas | Elimina el `payload JSONB` y permite consultar y tipar los datos directamente en `src`. Cada columna del CSV tiene su columna física en `snake_case`. |
| Normalizar los nombres a `snake_case` con truncado determinista | PostgreSQL limita los identificadores a 63 bytes. Los nombres que exceden el límite se truncan y reciben un sufijo hash de 8 caracteres para mantener la unicidad. |
| Usar tipos SQL nativos (`TEXT`, `INTEGER`, `NUMERIC`, `DATE`) | El tipado físico se resuelve en la carga Raw; dbt solo aplica reglas de negocio. |
| Guardar las cabeceras originales en `src.dataset_files.header_lines` | Conserva los nombres de campo exactos, incluidas cabeceras multinivel. |
| Ignorar las filas de cabecera en la carga | Las cabeceras se guardan como metadatos; no se insertan como filas de datos. |
| Completar las geografías exportadas por bloques en un paso separado | El loader Raw conserva el CSV íntegro; el relleno es una corrección de dimensión explícita, validada e idempotente, que no altera ninguna otra columna. |
| Identificar cada registro con `record_origin` | Permite distinguir datos reales de datos sintéticos sin mezclar su procedencia. |
| Añadir `synthetic_run_id` y `synthetic_period` | Trazabilidad de cada proyección temporal; el modelo utilizado se registra en `audit.load_log.model_name`. |
| Usar `PARSynthesizer` para series temporales | Modela secuencias y respeta el orden temporal de las series agregadas. |
| Usar `GaussianCopulaSynthesizer` o `CTGANSynthesizer` para datos estáticos | Genera variantes tabulares cuando no existe una dimensión temporal que proyectar. |
| No usar `HMASynthesizer` en esta fase | No se crean tablas relacionales nuevas; SDV opera sobre tablas Raw independientes. |
| Usar `row_id` y `loaded_at` como cursor incremental | Airbyte podrá capturar los registros sintéticos añadidos después de cada sincronización. |
| Roles `src_admin`, `src_writer` y `src_reader` | Principio de mínimo privilegio y separación de funciones. |
| Volumen Docker dedicado | Persistencia de datos entre reinicios. |

---

## 6. Pasos de implementación

### Paso 1 — Renombrar la fase y preparar el entorno

**Qué hacer**

1. Verificar que Docker y Docker Compose están disponibles.
2. Crear `.env.example` con las variables de conexión y contraseñas de roles.
3. Añadir `.env` al `.gitignore`.
4. Definir convenciones:
   - Nombres de tablas y columnas en `snake_case`.
   - Claves primarias con sufijo `_id`.
   - Timestamps en `TIMESTAMPTZ`.
   - **Sin `payload JSONB` en las tablas Raw:** una columna física por cada campo del CSV, con tipo SQL nativo.
   - Nombres de columna limitados a 63 bytes; truncado determinista con sufijo hash si la cabecera original es más larga.
   - Metadatos de origen y proyección en columnas de auditoría separadas.

**Archivos**

- `.env.example`
- `.gitignore` (raíz)

**Variables previstas**

```dotenv
POSTGRES_DB=source_talento
POSTGRES_USER=src_admin
POSTGRES_PASSWORD=cambiar_en_local
SRC_WRITER_USER=src_writer
SRC_WRITER_PASSWORD=cambiar_en_local
SRC_READER_USER=src_reader
SRC_READER_PASSWORD=cambiar_en_local
```

**Validación**

- La carpeta `01_source_system/` existe y contiene los CSV.
- `docker --version` y `docker compose version` responden correctamente.
- `.env.example` no contiene secretos reales.

---

### Paso 2 — Diseñar el esquema `src` y el modelo Raw

**Qué hacer**

1. Crear el esquema `src` para las tablas Raw de los CSV.
2. Crear el esquema `audit` para trazabilidad.
3. Crear `src.dataset_files` con los metadatos técnicos de cada fichero.
4. Crear una tabla `src.<dataset>_raw` por cada CSV.
5. Definir en cada tabla Raw:
   - Una columna física `snake_case` por cada columna útil del CSV.
   - Tipos SQL nativos (`TEXT`, `INTEGER`, `NUMERIC`, `DATE`) inferidos de los valores reales.
   - Las columnas de auditoría `row_id`, `load_id`, `row_number`, `record_origin`, `synthetic_run_id`, `synthetic_period` y `loaded_at`.
6. Definir índices y restricciones de unicidad.
7. **No crear ningún esquema ni tabla adicional para datos sintéticos.**

**Tablas Raw**

| Tabla Raw | Fichero origen |
| :--- | :--- |
| `src.cost_of_living_raw` | `(Datos Externos)  costo de vida global -  Kaggle.csv` |
| `src.foreign_affiliates_gender_raw` | `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` |
| `src.eurostat_hitech_employment_raw` | `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` |
| `src.epa_labor_market_raw` | `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` |
| `src.istac_employment_cnae_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` |
| `src.istac_employment_divisions_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` |
| `src.istac_employment_status_raw` | `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` |
| `src.foreign_affiliates_cnae_raw` | `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` |
| `src.tech_salaries_raw` | `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` |
| `src.emigration_pere_raw` | `(Datos Externos) Datos de Emigración - INE.csv` |

**Validación**

- Existen los esquemas `src` y `audit`.
- Existen las 10 tablas Raw y `src.dataset_files`.
- No existe ningún esquema ni tabla adicional para datos sintéticos.
- Cada tabla Raw tiene una columna física por campo del CSV y **ninguna columna `payload`**.
- Las columnas de auditoría están presentes en las 10 tablas Raw.
- Las cabeceras originales se conservan en `src.dataset_files.header_lines`.

---

### Paso 3 — Crear el contenedor PostgreSQL y la orquestación

**Qué hacer**

1. Crear `01_source_system/Dockerfile` basado en `postgres:16-alpine`.
2. Copiar los scripts de inicialización a `/docker-entrypoint-initdb.d`.
3. Declarar en `docker-compose.yml` el servicio `source_db` con:
   - Variables desde `.env`.
   - Volumen `source_pg_data`.
   - Montaje de `data_sources` en modo **solo lectura**.
   - `healthcheck` con `pg_isready`.
   - Red compartida para futuras fases.
4. Exponer el puerto `5432` solo en local.

**Archivos**

- `01_source_system/Dockerfile`
- `docker-compose.yml` (raíz)

**Validación**

- `docker compose up -d source_db` levanta el servicio en estado `healthy`.
- `docker compose logs source_db` no muestra errores de inicialización.
- El volumen persiste tras `docker compose restart`.
- El montaje de `data_sources` es de solo lectura.

---

### Paso 4 — Implementar los scripts de inicialización SQL

**Qué hacer**

1. `00_bootstrap_roles.sh`: crear roles y contraseñas desde variables de entorno.
2. `01_create_schemas.sql`: crear `src` y `audit`.
3. `02_create_tables.sql`: DDL de `src.dataset_files`, las 10 tablas Raw con columnas físicas tipadas y `audit.load_log`.
4. `03_indexes_constraints.sql`: índices, unicidad y restricciones.
5. `04_grants.sql`: permisos por rol.

El DDL de las tablas Raw se genera a partir de las cabeceras reales de cada CSV. No incluye ninguna columna `payload JSONB`: cada celda del fichero tiene su columna física en la tabla destino.

**Orden de ejecución**

Los scripts se ejecutan alfabéticamente por el entrypoint de PostgreSQL. Por eso el prefijo numérico es obligatorio.

**Validación**

- `\dn` muestra los esquemas `src` y `audit`.
- `\dt src.*` y `\dt audit.*` listan las tablas esperadas.
- Reiniciar con volumen vacío vuelve a crear todo correctamente.

---

### Paso 5 — Cargar los CSV en columnas físicas tipadas

**Qué hacer**

1. Crear `loaders/load_sources_raw.py`.
2. Definir `DATASETS_CATALOG` con los metadatos técnicos de lectura y el mapeo explícito a columnas físicas:
   - `file_name`: ruta exacta del CSV.
   - `encoding`: `utf-8-sig` o `latin-1`.
   - `delimiter`: `;` o `,`.
   - `header_rows`: número de filas de cabecera que hay que descartar y conservar como metadatos.
   - `table_name`: tabla Raw destino.
   - `decimal_style`: `dot` o `comma` (formato español).
   - `temporal_type`, `time_field`, `series_keys` y `value_fields`, expresados con **nombres físicos de columna**.
   - `columns`: lista de tuplas `(nombre_fisico, indice_csv, tipo_sql)`.
3. Abrir cada fichero en **modo lectura** y comprobar su SHA-256 antes de cargar. Si el fichero es un puntero de Git LFS, abortar con un mensaje accionable.
4. Leer con `csv.reader` ignorando las `header_rows` primeras filas, que se guardan íntegras en `header_lines`.
5. Convertir cada celda al tipo SQL de su columna:
   - `text`: se conserva el valor original.
   - `integer`: `INTEGER`; celda vacía → `NULL`.
   - `numeric`: `NUMERIC`; celda vacía → `NULL`; en `decimal_style = 'comma'` se eliminan los puntos de miles y se sustituye la coma por punto.
   - `date`: reservado para futuras fuentes.
6. Insertar cada fila directamente en sus columnas físicas con `execute_values`, desempaquetando la tupla en el mismo orden que `columns`.
7. Calcular el tamaño de página en función del número de columnas para no superar el límite de parámetros de PostgreSQL.
8. Guardar en `src.dataset_files`:
   - Nombre exacto del fichero.
   - Encoding y delimitador usados.
   - SHA-256.
   - Número de filas.
   - `header_lines` con las cabeceras originales exactas.
   - `data_start_row` como metadato para dbt.
   - `temporal_type`, `time_field`, `series_keys` y `value_fields` (nombres físicos) para la proyección SDV.
9. Registrar la carga en `audit.load_log` con `run_type = 'raw_load'`.
10. Hacer la carga idempotente por `file_hash`: relanzar el script no debe duplicar filas. `--force` borra las filas reales del dataset y lo recarga.

**Qué NO hace el loader**

- No modifica los CSV de `data_sources/`.
- No hace `melt`, `pivot`, `join` ni deduplicación.
- No traduce valores ni normaliza categorías.
- No aplica reglas de negocio (eso corresponde a dbt).
- No elimina filas: todas las filas de datos del CSV se insertan.
- No escribe en las tablas Raw con `payload JSONB`.

**Ejemplo de fila física real**

Para `src.foreign_affiliates_gender_raw`:

```text
row_id | load_id | row_number | periodo | ccaa  | total_regimen_total_sexo_total_ue_y_no_ue | ... | record_origin | synthetic_run_id | synthetic_period | loaded_at
-------+---------+------------+---------+-------+-------------------------------------------+-----+---------------+------------------+------------------+-----------
   123 | uuid    |          4 |  202601 | TOTAL |                               2644426.35 | ... | real          | NULL             | NULL             | now()
```

**Validación**

- El SHA-256 calculado coincide con el del fichero original.
- El número de filas cargadas coincide con el número de filas de datos del CSV.
- Las cabeceras originales están íntegras en `src.dataset_files.header_lines`.
- Cada fila se ha insertado en las columnas físicas correctas, sin `payload`.
- Las celdas vacías de columnas numéricas son `NULL`.
- `audit.load_log` refleja `status = 'success'` para los 10 ficheros (o `failed` con `error_message` si alguno no se puede leer).
- Relanzar el loader no duplica registros.

---

### Paso 5 bis — Completar las dimensiones geográficas exportadas por bloques

**Detección**

Al perfilar las columnas vacías de la capa Raw se detectaron dos tablas en las que `geography` solo viene informada en la primera fila de cada bloque; el resto de filas del bloque la heredan de forma implícita. El CSV no repite el valor, pero la dimensión es imprescindible para preparar las series temporales.

| Tabla Raw | Filas reales | `geography` vacía | Bloques | Tamaño del bloque |
| :--- | ---: | ---: | ---: | ---: |
| `src.istac_employment_cnae_raw` | 990 | 981 | 9 | 110 filas (105 actividades + 5 duplicados exactos) |
| `src.istac_employment_status_raw` | 315 | 210 | 105 | 3 filas (`Total` + 2 situaciones) |

El resto de columnas vacías detectadas son legítimas y **no se rellenan**:

- Jerarquía geográfica de PERE (`comunidades_y_ciudades_autonomas`, `provincias`, `total`), donde cada nivel tiene su propia granularidad y sus propias ausencias.
- Metadatos y flags de Eurostat (`time_label`, `obs_value_label`, `conf_status`, `obs_flag`, etc.).
- Nulos numéricos reales de ISTAC (series sin dato en un periodo concreto).

**Qué hacer**

1. Crear `loaders/complete_export_dimensions.py` con un catálogo `FILL_RULES` que declare, por tabla, la columna de geografía, la clave de serie, el número de bloques esperado y el tamaño de cada bloque.
2. Validar antes de escribir:
   - la columna `geography` existe y es `TEXT`;
   - no hay geografías compuestas solo por espacios;
   - el número de filas reales es múltiplo del tamaño de bloque;
   - cada bloque posicional contiene una única geografía no vacía;
   - cada bloque repite la misma secuencia de la clave de serie (`activity` o `employment_status`).
3. Rellenar solo las celdas `NULL` o `''` de `geography` en filas con `record_origin = 'real'`. Nunca se sobrescribe un valor existente ni se tocan filas sintéticas.
4. Calcular una huella MD5 de todas las columnas salvo `geography` (incluidas las filas sintéticas) antes y después del `UPDATE`. Si la huella cambia, hacer `ROLLBACK`.
5. Ejecutar por defecto en dry-run y escribir solo con `--apply`. El script es idempotente.

**Validación realizada**

- `istac_employment_cnae`: 981 celdas completadas, 9 geografías distintas y 0 celdas vacías restantes.
- `istac_employment_status`: 210 celdas completadas, 105 geografías distintas y 0 celdas vacías restantes.
- La huella de columnas no objetivo no cambia en ninguna de las dos tablas.
- Una segunda ejecución detecta 0 celdas pendientes en ambas tablas.
- Los 45 duplicados exactos de `istac_employment_cnae_raw` no se eliminan en este paso; se deduplicarán en la extracción SDV (`prepare_own_data.py`).

**Comandos**

```bash
# Simulación (comportamiento por defecto)
python 01_source_system/loaders/complete_export_dimensions.py

# Aplicar el relleno
python 01_source_system/loaders/complete_export_dimensions.py --apply

# Procesar una sola tabla
python 01_source_system/loaders/complete_export_dimensions.py \
  --dataset istac_employment_cnae --apply
```

---

### Paso 6 — Preparar las series temporales y los metadatos SDV

**Qué hacer**

1. Crear `synthetic_data/config.yaml` con:
   - Semilla.
   - Horizonte de proyección por frecuencia (`monthly`, `quarterly`, `annual`).
   - Modelo SDV por dataset.
   - Adaptadores de extracción temporal.
2. Crear `synthetic_data/prepare_own_data.py` para:
   - Leer `src.dataset_files` y las tablas Raw.
   - Extraer **en memoria** una tabla tabular indexada por tiempo por cada dataset proyectable.
   - Validar que la extracción respeta las cabeceras y los valores originales.
   - Generar `metadata.json` con la descripción SDV.
3. Crear `synthetic_data/metadata.json` con:
   - `sequence_key`: identificador de la serie (por ejemplo, CCAA + sección CNAE).
   - `sequence_index`: periodo temporal.
   - `context_columns`: columnas estáticas de la serie.
   - `sdtypes` de cada columna.
4. **No crear tablas nuevas en PostgreSQL.** La extracción es en memoria y el metadata es un fichero.

**Clasificación temporal de los datasets**

| Dataset | Formato | Columna temporal | Tipo | Modelo SDV previsto |
| :--- | :--- | :--- | :--- | :--- |
| `cost_of_living` | Cross-sectional | Ninguna | Estático | `GaussianCopulaSynthesizer` o `CTGANSynthesizer` (opcional) |
| `foreign_affiliates_gender` | Ancho | `periodo` (YYYYMM) | Mensual | `PARSynthesizer` |
| `eurostat_hitech_employment` | Largo | `time_period` | Anual | `PARSynthesizer` |
| `epa_labor_market` | Cross-sectional | Ninguna | Estático | `GaussianCopulaSynthesizer` o `CTGANSynthesizer` (opcional) |
| `istac_employment_cnae` | Ancho | Trimestres en columnas `*_AAAA_*_trimestre` | Trimestral | `PARSynthesizer` con extracción larga |
| `istac_employment_divisions` | Ancho | Columnas `empleo_AAAA_*_trimestre` | Trimestral | `PARSynthesizer` con extracción larga |
| `istac_employment_status` | Ancho | Columnas `empleo_AAAA_*_trimestre` | Trimestral | `PARSynthesizer` con extracción larga |
| `foreign_affiliates_cnae` | Ancho | `periodo` (YYYYMM) | Mensual | `PARSynthesizer` |
| `tech_salaries` | Largo | `work_year` (YYYY) | Anual (longitudinal) | `PARSynthesizer` (por `work_year`) o modelo tabular si se usa como benchmark estático |
| `emigration_pere` | Largo | `periodo` (YYYY) | Anual | `PARSynthesizer` (requiere parsear `total`, que se conserva como `TEXT`) |

**Metadatos SDV (ejemplo para un dataset largo)**

Los nombres del metadata son los **nombres físicos** de las columnas Raw:

```json
{
  "tables": {
    "eurostat_hitech_employment": {
      "sequence_key": "series_id",
      "sequence_index": "time_period",
      "columns": {
        "series_id": { "sdtype": "id" },
        "time_period": { "sdtype": "datetime", "datetime_format": "%Y" },
        "obs_value": { "sdtype": "numerical" },
        "geo": { "sdtype": "categorical" },
        "isco08": { "sdtype": "categorical" },
        "age": { "sdtype": "categorical" }
      }
    }
  }
}
```

**Configuración de ejemplo**

```yaml
seed: 42

horizon:
  monthly: 12
  quarterly: 8
  annual: 5

datasets:
  foreign_affiliates_gender:
    model: par
    frequency: monthly
    time_field: periodo
    series_keys: [ccaa]
    value_fields:
      - total_regimen_total_sexo_total_ue_y_no_ue
      - total_regimen_total_sexo_union_europea
      - total_regimen_total_sexo_no_union
  eurostat_hitech_employment:
    model: par
    frequency: annual
    time_field: time_period
    series_keys: [freq, category, isco08, age, unit, geo]
    value_fields: [obs_value]
  tech_salaries:
    model: par
    frequency: annual
    time_field: work_year
    series_keys:
      - experience_level
      - employment_type
      - job_title
      - salary_currency
      - employee_residence
      - company_location
      - company_size
    value_fields: [salary, salary_in_usd, remote_ratio]
  emigration_pere:
    model: par
    frequency: annual
    time_field: periodo
    series_keys:
      - pais_de_residencia
      - total_nacional
      - comunidades_y_ciudades_autonomas
      - provincias
      - sexo
      - lugar_de_nacimiento
    value_fields: []   # total se conserva como TEXT; se parsea en la extracción SDV
```

**Validación**

- La extracción temporal reconstruye correctamente los periodos de cada serie.
- El metadata SDV valida sin errores.
- No se ha creado ninguna tabla nueva en PostgreSQL.
- Los CSV originales siguen intactos.

---

### Paso 7 — Entrenar SDV y simular los registros futuros

**Qué hacer**

1. Crear `synthetic_data/simulate_future_raw_data.py`.
2. Para cada dataset proyectable:
   - Cargar la serie temporal extraída en memoria.
   - Entrenar `PARSynthesizer` con el metadata correspondiente.
   - Generar `horizon` periodos futuros por serie.
   - Guardar el modelo entrenado en `synthetic_data/models/`.
3. Para datasets estáticos o cross-sectional:
   - Usar `GaussianCopulaSynthesizer` o `CTGANSynthesizer` si se necesita ampliar la muestra.
   - No generar periodos futuros, porque no existe un eje temporal que proyectar.
4. Insertar cada registro sintético en la **misma tabla Raw** del dataset, escribiendo directamente en sus columnas físicas.
5. Registrar la ejecución en `audit.load_log` con `run_type = 'synthetic_projection'`, `model_name`, `synthetic_run_id` y `horizon`.

**Contrato de inserción sintética**

| Tipo de dataset | Contrato |
| :--- | :--- |
| Largo | Se rellenan las mismas columnas físicas que en una fila real; la columna temporal contiene el periodo futuro. |
| Ancho con eje temporal en `periodo` (TGSS) | Se inserta una nueva fila con el `periodo` futuro y el resto de columnas físicas rellenadas con los valores proyectados. |
| Ancho con periodos en columnas (ISTAC) | Se inserta una nueva fila por serie con las columnas de periodo existentes rellenadas. Si el horizonte incluye un periodo que aún no tiene columna, se añade con `ALTER TABLE ... ADD COLUMN` antes de insertar (ver nota). |
| Estático | Registro sintético con la misma estructura tabular del CSV; sin periodo futuro. |

> **Periodos futuros en datasets anchos (ISTAC):** la tabla conserva una columna física por periodo. Para proyectar un trimestre que aún no existe, el script de simulación debe ejecutar `ALTER TABLE src.<dataset>_raw ADD COLUMN IF NOT EXISTS <nueva_columna> NUMERIC` antes de insertar. Así se mantiene el modelo de columnas físicas, sin introducir `payload JSONB` ni `synthetic_layout`.

**Marcado de registros sintéticos**

Cada registro insertado tendrá:

```text
record_origin     = 'synthetic'
synthetic_run_id  = 'uuid-de-la-ejecucion'
synthetic_period  = '2027-01'
loaded_at         = now()
```

El modelo utilizado se registra en `audit.load_log.model_name`; no se duplica en la tabla Raw.

**Validación**

- El modelo SDV se entrena sin errores y se guarda en disco.
- Se generan los periodos futuros definidos en el horizonte.
- No se sobrescribe ni elimina ningún registro real.
- Los registros sintéticos quedan identificados por `record_origin = 'synthetic'`.
- Los valores sintéticos se escriben en columnas físicas, no en un `payload`.
- La proyección es reproducible con la misma semilla y el mismo modelo.

---

### Paso 8 — Validar integridad, fidelidad y continuidad temporal

**Qué hacer**

1. Validar que los CSV no han sido modificados.
2. Validar la carga Raw real.
3. Validar la proyección sintética.
4. Validar que Airbyte podrá capturar los incrementales.

**Consultas mínimas de carga Raw**

```sql
-- Ficheros cargados y hashes
SELECT dataset_name, source_file, row_count, sha256, loaded_at
FROM src.dataset_files
ORDER BY dataset_name;

-- Registros reales por dataset
SELECT 'foreign_affiliates_gender' AS dataset, COUNT(*) AS filas
FROM src.foreign_affiliates_gender_raw
WHERE record_origin = 'real'
UNION ALL
SELECT 'eurostat_hitech_employment', COUNT(*)
FROM src.eurostat_hitech_employment_raw
WHERE record_origin = 'real';

-- Cabeceras originales preservadas
SELECT dataset_name, header_lines
FROM src.dataset_files
WHERE dataset_name = 'foreign_affiliates_gender';

-- Columnas físicas (no debe existir ninguna columna payload)
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'src'
  AND table_name LIKE '%_raw'
ORDER BY table_name, ordinal_position;

-- Muestra de filas físicas tipadas
SELECT periodo, ccaa, total_regimen_total_sexo_total_ue_y_no_ue
FROM src.foreign_affiliates_gender_raw
ORDER BY row_id
LIMIT 5;
```

**Consultas mínimas de proyección sintética**

```sql
-- Registros sintéticos por ejecución
SELECT synthetic_run_id, dataset_name, model_name, horizon, rows_loaded, status
FROM audit.load_log
WHERE run_type = 'synthetic_projection'
ORDER BY started_at DESC;

-- Periodos futuros generados
SELECT synthetic_period, COUNT(*) AS registros
FROM src.foreign_affiliates_gender_raw
WHERE record_origin = 'synthetic'
GROUP BY synthetic_period
ORDER BY synthetic_period;

-- Duplicados de periodo por serie
SELECT synthetic_period, ccaa, COUNT(*)
FROM src.foreign_affiliates_gender_raw
WHERE record_origin = 'synthetic'
GROUP BY synthetic_period, ccaa
HAVING COUNT(*) > 1;
```

**Criterios de calidad**

- El SHA-256 de cada CSV coincide con el original.
- El número de filas reales coincide con el número de filas del CSV.
- Las cabeceras originales están completas en `src.dataset_files.header_lines`.
- Las tablas Raw no tienen columna `payload`.
- Los registros sintéticos solo añaden periodos posteriores al último periodo real.
- No hay duplicados de periodo por serie.
- Los valores sintéticos respetan rangos plausibles y la agregación de la serie.
- Ningún registro real ha sido modificado ni eliminado.
- `row_id` y `loaded_at` permiten a Airbyte detectar los nuevos registros.

---

### Paso 9 — Documentar y preparar el handoff a la Fase 2

**Qué hacer**

1. Actualizar el `README.md` de la fase con:
   - Comandos definitivos de arranque.
   - Estructura final de directorios.
   - Descripción de `src.dataset_files`, las tablas Raw y `audit.load_log`.
   - Modelos SDV elegidos, horizonte temporal y comandos de simulación.
   - Credenciales de ejemplo y rol recomendado para Airbyte.
2. Añadir un apartado de troubleshooting:
   - Reinicializar volumen.
   - Reejecutar el loader Raw.
   - Reentrenar o regenerar la proyección temporal.
3. Documentar el contrato de datos para Airbyte:
   - Esquemas a replicar: `src` y `audit`.
   - Tablas con clave primaria.
   - Cursor incremental: `row_id` y `loaded_at`.
   - Distinción entre registros reales y sintéticos.
4. Entregar a la Fase 2:
   - Host, puerto, base de datos.
   - Usuario `src_reader` y contraseña.
   - Lista de tablas, volúmenes reales y periodos sintéticos generados.

**Validación**

- Un tercero puede levantar la fase desde cero siguiendo el README.
- Airbyte puede conectarse con `src_reader` y descubrir `src` y `audit`.
- Los volúmenes documentados coinciden con la realidad.

---

## 7. Modelo de datos detallado

### 7.1 `src.dataset_files`

Metadatos de cada fichero. No contiene datos transformados.

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `dataset_name` | `VARCHAR(80)` | PK |
| `source_file` | `TEXT` | NOT NULL |
| `encoding` | `VARCHAR(20)` | NOT NULL |
| `delimiter` | `CHAR(1)` | NOT NULL |
| `header_lines` | `JSONB` | Cabeceras originales exactas |
| `data_start_row` | `INTEGER` | Fila donde empiezan los datos |
| `row_count` | `INTEGER` | Filas totales leídas |
| `sha256` | `CHAR(64)` | NOT NULL |
| `temporal_type` | `VARCHAR(20)` | `monthly`, `quarterly`, `annual`, `static` |
| `time_field` | `TEXT` | Nombre físico de la columna temporal (`periodo`, `time_period`, `work_year`) o `NULL` |
| `series_keys` | `JSONB` | Nombres físicos de las columnas que identifican la serie |
| `value_fields` | `JSONB` | Nombres físicos de las columnas numéricas proyectables |
| `loaded_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

### 7.2 `src.<dataset>_raw`

Una tabla por fichero. Cada tabla combina:

1. **Columnas físicas del CSV:** una por cada campo, con nombre normalizado `snake_case` y tipo SQL nativo.
2. **Columnas de auditoría y proyección**, comunes a las 10 tablas.

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `row_id` | `BIGSERIAL` | PK |
| `load_id` | `UUID` | NOT NULL |
| `row_number` | `INTEGER` | NOT NULL |
| `<columna_fisica_1>` | `TEXT` / `INTEGER` / `NUMERIC` / `DATE` | NULL si la celda está vacía y el tipo no es `TEXT` |
| `...` | ... | ... |
| `record_origin` | `VARCHAR(20)` | NOT NULL DEFAULT `'real'`, CHECK (`real`, `synthetic`) |
| `synthetic_run_id` | `UUID` | NULL para registros reales |
| `synthetic_period` | `VARCHAR(30)` | NULL para registros reales |
| `loaded_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT `NOW()` |

Ejemplo (extracto de `src.foreign_affiliates_gender_raw`):

```sql
CREATE TABLE IF NOT EXISTS src.foreign_affiliates_gender_raw (
    row_id BIGSERIAL PRIMARY KEY,
    load_id UUID NOT NULL,
    row_number INTEGER NOT NULL,
    periodo INTEGER,
    ccaa TEXT,
    total_regimen_total_sexo_total_ue_y_no_ue NUMERIC,
    ...
    record_origin VARCHAR(20) NOT NULL DEFAULT 'real'
        CHECK (record_origin IN ('real', 'synthetic')),
    synthetic_run_id UUID,
    synthetic_period VARCHAR(30),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Restricción de unicidad:

```sql
UNIQUE (load_id, row_number)
```

Índices recomendados:

```sql
CREATE INDEX idx_<dataset>_raw_origin ON src.<dataset>_raw (record_origin);
CREATE INDEX idx_<dataset>_raw_run ON src.<dataset>_raw (synthetic_run_id);
CREATE INDEX idx_<dataset>_raw_loaded_at ON src.<dataset>_raw (loaded_at);
CREATE INDEX idx_<dataset>_raw_row_id ON src.<dataset>_raw (row_id);
```

**Número de columnas físicas por tabla**

| Tabla Raw | Columnas físicas | Columnas totales (físicas + 7 de auditoría) |
| :--- | ---: | ---: |
| `src.cost_of_living_raw` | 58 | 65 |
| `src.foreign_affiliates_gender_raw` | 110 | 117 |
| `src.eurostat_hitech_employment_raw` | 23 | 30 |
| `src.epa_labor_market_raw` | 9 | 16 |
| `src.istac_employment_cnae_raw` | 257 | 264 |
| `src.istac_employment_divisions_raw` | 41 | 48 |
| `src.istac_employment_status_raw` | 139 | 146 |
| `src.foreign_affiliates_cnae_raw` | 278 | 285 |
| `src.tech_salaries_raw` | 11 | 18 |
| `src.emigration_pere_raw` | 8 | 15 |

**Normalización de nombres y límite de 63 bytes**

- Se eliminan acentos, se pasa a minúsculas y se sustituyen los caracteres no alfanuméricos por `_`.
- Las cabeceras multinivel se aplanan combinando periodo y medida. Ejemplo:
  `Empleos por cuenta ajena` + `2025 Cuarto trimestre` → `empleos_por_cuenta_ajena_2025_cuarto_trimestre`.
- PostgreSQL limita los identificadores a **63 bytes**. Los nombres que superan ese límite se truncan de forma determinista y reciben un sufijo hash SHA-1 de 8 caracteres para garantizar la unicidad.
- Las cabeceras originales completas, sin truncar, se conservan en `src.dataset_files.header_lines`.

**Por qué no se usan los nombres originales literales**

Los nombres originales de TGSS e ISTAC contienen espacios, acentos, paréntesis, símbolos y, en muchos casos, superan los 63 bytes. Usarlos literalmente obligaría a citarlos siempre entre comillas, provocaría colisiones por truncamiento y dificultaría las consultas. La normalización `snake_case` con sufijo hash mantiene la unicidad y la trazabilidad, mientras que `header_lines` preserva el nombre original exacto.

### 7.3 `audit.load_log`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `load_id` | `UUID` | PK |
| `dataset_name` | `VARCHAR(80)` | NOT NULL |
| `source_file` | `TEXT` | NOT NULL |
| `file_hash` | `CHAR(64)` | NOT NULL |
| `rows_loaded` | `INTEGER` | NOT NULL |
| `run_type` | `VARCHAR(30)` | `raw_load` o `synthetic_projection` |
| `model_name` | `VARCHAR(40)` | NULL en cargas reales |
| `synthetic_run_id` | `UUID` | NULL en cargas reales |
| `horizon` | `INTEGER` | NULL en cargas reales |
| `status` | `VARCHAR(20)` | `success`, `failed` |
| `started_at` | `TIMESTAMPTZ` | NOT NULL |
| `finished_at` | `TIMESTAMPTZ` | NULL |
| `error_message` | `TEXT` | NULL |

---

## 8. Mapeo de fuentes CSV a tablas Raw y estrategia temporal

> **Importante:** las cabeceras originales se conservan en `src.dataset_files.header_lines`. Cada celda se inserta en una columna física `snake_case` con tipo SQL nativo. La columna “Lectura técnica” describe cómo se abre y se desempaqueta el fichero.

| Fichero exacto | Encoding | Separador | Filas de cabecera | Tabla Raw destino | Columnas físicas | Lectura técnica | Proyección temporal |
| :--- | :--- | :--- | :--- | :--- | ---: | :--- | :--- |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | `utf-8-sig` | `,` | 1 | `src.cost_of_living_raw` | 58 | `city`/`country` como `TEXT`; `x1..x55` y `data_quality` como `NUMERIC`/`INTEGER` | No aplica (estático) |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` | `latin-1` | `;` | 3 (título + línea vacía + cabecera) | `src.foreign_affiliates_gender_raw` | 110 | `periodo` `INTEGER`, `ccaa` `TEXT`, 108 medidas `NUMERIC` | `PARSynthesizer` mensual |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | `utf-8-sig` | `,` | 1 | `src.eurostat_hitech_employment_raw` | 23 | Dimensiones `TEXT`, `time_period` `INTEGER`, `obs_value` `NUMERIC` | `PARSynthesizer` anual |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.epa_labor_market_raw` | 9 | `region` `TEXT`; 8 medidas `NUMERIC` con coma decimal convertida | No aplica (estático) |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.istac_employment_cnae_raw` | 257 | `geography`/`activity` `TEXT`; 255 medidas periodo × tipo de empleo `NUMERIC` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | `utf-8-sig` | `,` | 1 | `src.istac_employment_divisions_raw` | 41 | `activity` `TEXT`; 40 periodos trimestrales `NUMERIC` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | `utf-8-sig` | `,` | 1 | `src.istac_employment_status_raw` | 139 | `geography`/`employment_status` `TEXT`; 137 periodos trimestrales `NUMERIC` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | `utf-8-sig` | `;` | 2 (título + cabecera) | `src.foreign_affiliates_cnae_raw` | 278 | `periodo` `INTEGER`, `ccaa` `TEXT`, 276 medidas `NUMERIC` | `PARSynthesizer` mensual |
| `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` | `utf-8-sig` | `,` | 1 | `src.tech_salaries_raw` | 11 | `work_year`, `salary`, `salary_in_usd`, `remote_ratio` numéricos; dimensiones `TEXT` | `PARSynthesizer` anual por `work_year` (o modelo tabular si se usa como benchmark estático) |
| `(Datos Externos) Datos de Emigración - INE.csv` | `utf-8-sig` | `;` | 1 | `src.emigration_pere_raw` | 8 | `periodo` `INTEGER`; dimensiones y `total` como `TEXT` (conserva separador de miles) | `PARSynthesizer` anual (stock por país/provincia) |

**Notas de tipado**

- `NUMERIC` se usa para medidas decimales limpias. `INTEGER` se usa para periodos, años, salarios y `remote_ratio`.
- `emigration_pere.total` se mantiene como `TEXT` porque el CSV usa separador de miles y celdas `""`; el parseo numérico se hará en la extracción SDV o en dbt.
- `DATE` está previsto en el conversor del loader, pero ninguna fuente actual lo necesita.
- Las celdas vacías en columnas `INTEGER`/`NUMERIC` se insertan como `NULL`. En `TEXT` se conserva la cadena original.
- Las exportaciones ISTAC de CNAE y situaciones solo informan `geography` en la primera fila de cada bloque. La capa Raw conserva esas celdas vacías y el Paso 5 bis las completa de forma controlada.

---

## 9. Estrategia SDV detallada

### 9.1 Extracción temporal en memoria

Para cada dataset proyectable, `prepare_own_data.py` construye una tabla tabular en memoria:

- **Largo con eje temporal** (TGSS, Eurostat y PERE): una fila por serie y periodo.
- **Largo longitudinal** (Salarios Tecnológicos): una fila por observación; `work_year` actúa como eje temporal.
- **Ancho** (ISTAC y TGSS 5mb25): se transpone en memoria a formato largo para entrenar la secuencia.
- **Estático** (Coste de vida y EPA): se mantiene como tabla tabular sin eje temporal.

La extracción no modifica los CSV ni las filas Raw. Solo genera una vista temporal de lectura para SDV.

### 9.2 Modelos SDV

| Mecanismo | Clase | Uso en esta fase |
| :--- | :--- | :--- |
| **Synthesize Sequences (PAR)** | `PARSynthesizer` | **Principal** para series temporales: aprende la secuencia histórica y proyecta periodos futuros. |
| **Synthesize a Table (Gaussian Copula)** | `GaussianCopulaSynthesizer` | Baseline tabular para datasets estáticos o cross-sectional. |
| **Synthesize a Table (CTGAN)** | `CTGANSynthesizer` | Alternativa de alta fidelidad para datasets estáticos con distribuciones complejas. |
| **Synthesize Multi Tables (HMA)** | `HMASynthesizer` | **No aplicable**: no se crean tablas relacionales nuevas. |
| **Use Your Own Data** | Carga de series + `metadata.json` | **Obligatorio** para preparar la vista temporal y describir el esquema antes de entrenar. |

### 9.3 Proyección temporal

1. `PARSynthesizer` aprende la secuencia histórica de cada serie.
2. Se genera un horizonte futuro configurable:
   - Mensual: 12 periodos.
   - Trimestral: 8 periodos.
   - Anual: 5 periodos.
3. Los periodos generados deben ser **estrictamente posteriores** al último periodo real de cada serie.
4. Los valores sintéticos se escriben directamente en las columnas físicas de la tabla Raw.
5. Los registros se insertan en la misma tabla Raw con `record_origin = 'synthetic'`, `synthetic_run_id` y `synthetic_period`.
6. En datasets anchos con periodos en columnas (ISTAC), los periodos nuevos requieren `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` antes de la inserción.

### 9.4 Evaluación de la síntesis

`synthetic_data/evaluate_synthetic.py` debe comprobar:

- Similitud estadística entre la serie histórica y la sintética.
- Continuidad temporal y ausencia de duplicados de periodo.
- Rangos plausibles y coherencia de agregación.
- Que no se hayan modificado filas reales.
- Que los registros sintéticos sean identificables y filtrables.

### 9.5 Reproducibilidad

- Semilla fija en `config.yaml`.
- Modelos entrenados guardados en `synthetic_data/models/`.
- Metadata y configuración versionados.
- Cada ejecución registrada en `audit.load_log` con `synthetic_run_id`.

---

## 10. Relación con las fases posteriores

| Fase | Uso de los datos de la Fase 1 |
| :--- | :--- |
| Fase 2 — Airbyte | Replica los registros reales y sintéticos de `src.*` usando `row_id` y `loaded_at` como cursor incremental. |
| Fase 3 — Raw Database | Almacena la copia Raw sin transformar, incluyendo los registros sintéticos. |
| Fase 4 — dbt | Consume las columnas físicas ya tipadas, separa reales de sintéticos, aplica reglas de negocio y calcula los indicadores macroeconómicos del proyecto. |

---

## 11. Criterios de aceptación

- [ ] La carpeta `01_source_system/` existe y contiene los 10 CSV.
- [ ] `docker compose up -d source_db` levanta el servicio en estado `healthy`.
- [ ] Existen los esquemas `src` y `audit`.
- [ ] Existen las 10 tablas Raw, `src.dataset_files` y `audit.load_log`.
- [ ] No existe ningún esquema ni tabla adicional para datos sintéticos.
- [ ] Cada tabla Raw tiene una columna física `snake_case` por campo del CSV y **ninguna columna `payload`**.
- [ ] Los tipos SQL nativos (`TEXT`, `INTEGER`, `NUMERIC`) coinciden con el contenido real de cada columna.
- [ ] Los nombres que superan 63 bytes se truncan con sufijo hash y las cabeceras completas están en `src.dataset_files.header_lines`.
- [ ] Los 10 CSV se cargan sin errores y con recuentos validados (PERE requiere `git lfs pull`).
- [ ] Relanzar el loader sin `--force` no duplica filas (idempotencia por SHA-256).
- [ ] `geography` está completo en `src.istac_employment_cnae_raw` (9 geografías) y `src.istac_employment_status_raw` (105 geografías).
- [ ] El relleno de `geography` es idempotente y no altera ninguna otra columna ni fila sintética.
- [ ] El SHA-256 de cada CSV coincide con el del fichero original.
- [ ] Las cabeceras originales están preservadas en `src.dataset_files.header_lines`.
- [ ] Ningún CSV ha sido modificado, renombrado ni transformado.
- [ ] El metadata SDV valida correctamente.
- [ ] `PARSynthesizer` entrena y genera periodos futuros para los datasets temporales.
- [ ] Los registros sintéticos se insertan en las mismas tablas Raw.
- [ ] Los registros sintéticos están marcados con `record_origin = 'synthetic'` y `synthetic_run_id`.
- [ ] No se ha modificado ni eliminado ningún registro real.
- [ ] La continuidad temporal y la ausencia de duplicados están validadas.
- [ ] `src_reader` solo puede leer; `src_writer` puede cargar y simular.
- [ ] Airbyte puede conectarse con `src_reader` y detectar nuevos registros por `row_id` y `loaded_at`.
- [ ] El README de la fase permite reproducir todo desde cero.

---

## 12. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
| :--- | :--- | :--- |
| Identificadores PostgreSQL de 63 bytes | Nombres truncados o colisiones | Normalizar a `snake_case`, truncar de forma determinista y añadir sufijo hash SHA-1 de 8 caracteres; conservar la cabecera exacta en `src.dataset_files.header_lines` |
| Encoding incorrecto en TGSS | Pérdida de caracteres al leer | Forzar `latin-1` para `3mb...` y `utf-8-sig` para el resto |
| Cabeceras multinivel en EPA/ISTAC | Interpretación errónea de columnas | Aplanar periodo + medida en el nombre físico; conservar las cabeceras originales en `header_lines` |
| Coma decimal en EPA | Valores alterados si se convierten mal | `decimal_style: comma` en el catálogo: eliminar puntos de miles y cambiar coma por punto antes de `Decimal` |
| Series anchas con periodos en columnas | Dificultad para proyectar periodos futuros | Extraer en memoria a formato largo, entrenar PAR e insertar en las columnas físicas; añadir columnas de periodo con `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` |
| Relleno de geografía que cruza límites de bloque | Asignar una geografía incorrecta a una serie | Agrupar por tamaño de bloque validado, comprobar que cada bloque repite la secuencia de la clave de serie y comparar la huella de columnas no objetivo antes/después |
| Reejecución del relleno de geografía | Sobrescribir valores o duplicar trabajo | Actualizar solo celdas `NULL`/`''` de filas reales, validar el resultado y dejar el dry-run como comportamiento por defecto |
| Filas con número de columnas inesperado | Inserción parcial o corrupta | El loader valida `len(row)` contra el máximo índice del catálogo y aborta el dataset; `audit.load_log` registra `failed` |
| Fichero PERE como puntero Git LFS | Carga fallida con error poco claro | El loader detecta el puntero y lanza un error accionable: `git lfs pull` |
| Entrenamiento PAR lento o con alto consumo de memoria | Bloqueo del entorno local | Ajustar épocas, tamaño de lote y horizonte; empezar con un dataset y escalar |
| Fichero PERE de ~4M filas y 333 MB | Carga lenta y alto consumo de memoria | Cargar por lotes con `execute_values`, calcular el tamaño de página según el número de columnas y no materializar todo el fichero en memoria |
| Salarios en múltiples monedas | Comparaciones salariales incorrectas | Conservar `salary` y `salary_currency`; usar `salary_in_usd` para comparaciones en dbt |
| Jerarquía geográfica excluyente en PERE | Duplicación del stock al agregar país y provincia | Documentar la jerarquía y filtrar por nivel geográfico en dbt |
| Registros sintéticos confundidos con reales | Análisis incorrectos | Marcar con `record_origin`, `synthetic_run_id` y `synthetic_period`; el modelo se registra en `audit.load_log.model_name` |
| Airbyte no detecta los nuevos registros | Incrementales perdidos | Usar `row_id` monotónico y `loaded_at` como cursor incremental |
| Los scripts de init solo corren con volumen vacío | Cambios no aplicados | Documentar `docker compose down -v` para reinicializar |
| Nombres de fichero con espacios | Errores de apertura | Rutas entre comillas y resolución con `pathlib` |
| Credenciales en el repositorio | Riesgo de seguridad | `.env` fuera de Git y `.env.example` sin secretos |

---

## 13. Comandos de referencia

```bash
# Levantar PostgreSQL 1
docker compose up -d source_db

# Ver estado y logs
docker compose ps
docker compose logs -f source_db

# Reinicializar desde cero (borra el volumen)
docker compose down -v
docker compose up -d source_db

# Cargar los CSV en columnas físicas (desde la raíz del repositorio)
python 01_source_system/loaders/load_sources_raw.py

# Recargar un dataset concreto
python 01_source_system/loaders/load_sources_raw.py --dataset tech_salaries --force

# Completar las geografías exportadas por bloques (dry-run por defecto)
python 01_source_system/loaders/complete_export_dimensions.py

# Aplicar el relleno de geografía
python 01_source_system/loaders/complete_export_dimensions.py --apply

# Verificar la carga
psql "$DATABASE_URL" -c "SELECT dataset_name, row_count, sha256, loaded_at FROM src.dataset_files ORDER BY dataset_name;"
psql "$DATABASE_URL" -c "SELECT dataset_name, rows_loaded, status, started_at FROM audit.load_log ORDER BY started_at DESC;"

# Preparar las series temporales y validar el metadata SDV
python 01_source_system/synthetic_data/prepare_own_data.py

# Simular periodos futuros para todos los datasets proyectables
python 01_source_system/synthetic_data/simulate_future_raw_data.py --all

# Simular un dataset concreto con horizonte personalizado
python 01_source_system/synthetic_data/simulate_future_raw_data.py \
  --dataset foreign_affiliates_gender \
  --horizon 12

# Evaluar la calidad de la proyección sintética
python 01_source_system/synthetic_data/evaluate_synthetic.py --all

# Conectarse como lector (puerto y credenciales según .env)
psql "host=localhost port=5432 dbname=source_talento user=src_reader password=..."
```

> Si el fichero PERE aparece como puntero de Git LFS, ejecutar `git lfs pull` antes de lanzar el loader.

---

## 14. Checklist de ejecución

| Paso | Descripción | Estado |
| :--- | :--- | :--- |
| 1 | Renombrar la fase y preparar el entorno | Hecho |
| 2 | Diseñar el esquema `src` y el modelo Raw físico | Hecho |
| 3 | Crear el contenedor `source_db` y la orquestación | Hecho |
| 4 | Implementar los scripts SQL de inicialización | Hecho |
| 5 | Cargar los CSV en columnas físicas tipadas | Hecho |
| 5 bis | Completar las dimensiones geográficas exportadas por bloques | Hecho |
| 6 | Preparar las series temporales y los metadatos SDV | Pendiente |
| 7 | Entrenar SDV y simular los registros futuros | Pendiente |
| 8 | Insertar el anexo sintético y validar | Pendiente |
| 9 | Documentar y preparar el handoff a la Fase 2 | Parcial (README actualizado) |

---

## 15. Handoff a la Fase 2

Al cerrar esta fase se entregará:

- Contenedor `source_db` operativo y persistente.
- Base de datos `source_talento` con esquemas `src` y `audit`.
- Tablas Raw `src.*` con los CSV reales íntegros y los registros sintéticos futuros.
- `src.dataset_files` con metadatos técnicos y temporales.
- `audit.load_log` con trazabilidad de cargas reales y proyecciones sintéticas.
- Metadatos, configuración y modelos SDV versionados.
- Usuario `src_reader` con permisos de solo lectura para Airbyte.
- Contrato de cursor incremental basado en `row_id` y `loaded_at`.
- README actualizado con instrucciones reproducibles.
- Este plan marcado con el estado real de ejecución.
