# Plan de Implementación — Fase 1: Source System, Carga Raw y Simulación Temporal con SDV

> **Proyecto:** Gestión de Fuga y Atracción de Nómadas Digitales Cualificados en Canarias y España  
> **Fase:** 1 de 4 — `01_source_system`  
> **Componente:** PostgreSQL 1 (sistema origen)  
> **Estado:** Planificado (v3 — corrección arquitectónica definitiva)  
> **Última actualización:** 2026-11-10

---

## 0. Principio rector

Esta fase construye **exclusivamente el sistema origen y su evolución temporal simulada**:

- Los 10 ficheros de `data_sources/` son **inmutables**: no se editan, recortan, renombran ni sobrescriben.
- Los CSV se cargan **en crudo** en el esquema `src.*`, preservando filas, cabeceras y valores originales.
- **No se crean tablas nuevas para datos sintéticos.** SDV aprende de las tablas Raw existentes y añade registros sintéticos a esas mismas tablas.
- **No se simulan tablas transaccionales ficticias.** No existen esquemas ni entidades de tipo operacional.
- La generación sintética es un **anexo temporal**: añade periodos futuros a las series históricas para que Airbyte pueda capturar incrementales en la Fase 2.
- Cualquier limpieza, tipado, normalización o modelado de negocio se realizará en la **Fase 4 (dbt)**.

---

## 1. Objetivo de la fase

Levantar el contenedor PostgreSQL del sistema origen, cargar los 10 ficheros CSV macroeconómicos reales en crudo en el esquema `src.*`, y aplicar modelos de **SDV (Synthetic Data Vault)** sobre estas tablas para generar registros sintéticos futuros que simulen la evolución temporal agregada de las series.

Al finalizar la fase:

- El sistema origen estará operativo y persistente.
- Los CSV reales estarán cargados sin transformación.
- Las tablas Raw contendrán tanto registros reales como registros sintéticos futuros, claramente identificables.
- Airbyte podrá conectarse como lector y capturar los nuevos registros mediante un cursor incremental.

---

## 2. Alcance

### Incluido

- Renombrar la carpeta de la fase a `01_source_system/`.
- Creación del contenedor PostgreSQL 1 y su volumen persistente.
- Esquema `src` para la carga Raw de los CSV.
- Esquema `audit` para trazabilidad de cargas y proyecciones.
- Carga íntegra de los 10 CSV sin transformarlos.
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
- Transformación, tipado, limpieza o normalización de los CSV.
- Configuración de conectores Airbyte (Fase 2).
- Creación de la base de datos Raw de destino (Fase 3).
- Modelos dbt y cálculo de indicadores (Fase 4).
- Cuadros de mando o visualización.

---

## 3. Estado inicial del repositorio

A fecha de este plan:

```text
01_source_system/
├── README.md
├── PLAN_IMPLEMENTACION_FASE_1.md
├── data_sources/          # 10 CSV reales ya descargados
├── init-scripts/          # vacío
└── synthetic_data/        # vacío
```

No existe todavía `docker-compose.yml`, `Dockerfile`, scripts SQL ni cargadores. Los CSV están disponibles y sus particularidades técnicas están documentadas en el `README.md` de la fase.

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
| Cargar los CSV como arrays `JSONB` | PostgreSQL trunca identificadores a 63 bytes; las cabeceras reales de TGSS son más largas. El array preserva valores y el metadata preserva nombres. |
| Guardar las cabeceras originales en `src.dataset_files.header_lines` | Conserva los nombres de campo exactos, incluidas cabeceras multinivel. |
| Cargar todas las filas, incluidas cabeceras y líneas vacías | Máxima fidelidad respecto al fichero original. |
| Identificar cada registro con `record_origin` | Permite distinguir datos reales de datos sintéticos sin mezclar su procedencia. |
| Añadir `synthetic_run_id`, `synthetic_period` y `synthetic_model` | Trazabilidad completa de cada proyección temporal. |
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
   - Nombres de tablas en `snake_case`.
   - Claves primarias con sufijo `_id`.
   - Timestamps en `TIMESTAMPTZ`.
   - JSONB para los payloads Raw.
   - Metadatos de origen y proyección en columnas separadas.

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
5. Definir índices y restricciones de unicidad.
6. **No crear ningún esquema ni tabla adicional para datos sintéticos.**

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
- Las tablas Raw no contienen columnas renombradas ni valores transformados.

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
3. `02_create_tables.sql`: DDL de `src.dataset_files`, las 10 tablas Raw y `audit.load_log`.
4. `03_indexes_constraints.sql`: índices, unicidad y restricciones.
5. `04_grants.sql`: permisos por rol.

**Orden de ejecución**

Los scripts se ejecutan alfabéticamente por el entrypoint de PostgreSQL. Por eso el prefijo numérico es obligatorio.

**Validación**

- `\dn` muestra los esquemas `src` y `audit`.
- `\dt src.*` y `\dt audit.*` listan las tablas esperadas.
- Reiniciar con volumen vacío vuelve a crear todo correctamente.

---

### Paso 5 — Cargar los CSV en crudo, sin transformarlos

**Qué hacer**

1. Crear `loaders/load_sources_raw.py`.
2. Definir un catálogo de ficheros con **solo metadatos técnicos de lectura**:
   - Ruta exacta del CSV.
   - Encoding de apertura (`utf-8-sig` o `latin-1`).
   - Delimitador (`;` o `,`).
   - Número de filas de cabecera.
   - Nombre de la tabla Raw destino.
3. Abrir cada fichero en **modo lectura** y comprobar su SHA-256 antes de cargar.
4. Leer todas las filas con `csv.reader` y almacenarlas como arrays `JSONB` de valores textuales en `src.<dataset>_raw`.
5. Guardar en `src.dataset_files`:
   - Nombre exacto del fichero.
   - Encoding y delimitador usados.
   - SHA-256.
   - Número de filas.
   - `header_lines` con las cabeceras originales exactas.
   - `data_start_row` como metadato para dbt.
   - `temporal_type`, `time_field`, `series_keys` y `value_fields` para la proyección SDV.
6. Registrar la carga en `audit.load_log` con `run_type = 'raw_load'`.
7. Hacer la carga idempotente por `file_hash`: relanzar el script no debe duplicar filas.

**Qué NO hace el loader**

- No renombra columnas.
- No convierte tipos.
- No elimina filas.
- No traduce valores.
- No normaliza formatos.
- No hace `melt`, `pivot`, `join` ni deduplicación.
- No escribe nunca en `data_sources/`.

**Ejemplo conceptual de fila Raw real**

```json
{
  "row_number": 4,
  "payload": ["202601", "TOTAL", "2644426.35", "838003.65", "1806422.70"],
  "record_origin": "real",
  "synthetic_run_id": null,
  "synthetic_period": null
}
```

**Validación**

- El SHA-256 calculado coincide con el del fichero original.
- El número de filas cargadas coincide con el número de filas leídas del CSV.
- Las cabeceras originales están íntegras en `src.dataset_files.header_lines`.
- No existe ninguna columna renombrada ni valor transformado.
- `audit.load_log` refleja `status = 'success'` para los 10 ficheros.
- Relanzar el loader no duplica registros.

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

| Dataset | Formato | Eje temporal | Tipo | Modelo SDV previsto |
| :--- | :--- | :--- | :--- | :--- |
| `cost_of_living` | Cross-sectional | Ninguno | Estático | `GaussianCopulaSynthesizer` o `CTGANSynthesizer` (opcional) |
| `foreign_affiliates_gender` | Ancho | `Período` (YYYYMM) | Mensual | `PARSynthesizer` |
| `eurostat_hitech_employment` | Largo | `TIME_PERIOD` | Anual | `PARSynthesizer` |
| `epa_labor_market` | Cross-sectional | Ninguno | Estático | `GaussianCopulaSynthesizer` o `CTGANSynthesizer` (opcional) |
| `istac_employment_cnae` | Ancho | Trimestres en cabecera | Trimestral | `PARSynthesizer` con extracción larga |
| `istac_employment_divisions` | Ancho | Trimestres en cabecera | Trimestral | `PARSynthesizer` con extracción larga |
| `istac_employment_status` | Ancho | Trimestres en cabecera | Trimestral | `PARSynthesizer` con extracción larga |
| `foreign_affiliates_cnae` | Ancho | `Período` (YYYYMM) | Mensual | `PARSynthesizer` |
| `tech_salaries` | Largo | `work_year` (YYYY) | Anual (longitudinal) | `PARSynthesizer` (por `work_year`) o modelo tabular si se usa como benchmark estático |
| `emigration_pere` | Largo | `Periodo` (YYYY) | Anual | `PARSynthesizer` |

**Metadatos SDV (ejemplo para un dataset largo)**

```json
{
  "tables": {
    "eurostat_hitech_employment": {
      "sequence_key": "series_id",
      "sequence_index": "period",
      "columns": {
        "series_id": { "sdtype": "id" },
        "period": { "sdtype": "datetime", "datetime_format": "%Y" },
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
    time_field: payload[0]
    series_keys: [1]
    value_fields: [2, 3, 4, 5, 6]
  eurostat_hitech_employment:
    model: par
    frequency: annual
    time_field: TIME_PERIOD
    series_keys: [geo, isco08, age]
    value_fields: [OBS_VALUE]
  tech_salaries:
    model: par
    frequency: annual
    time_field: payload[0]
    series_keys: [1, 2, 3, 5, 7, 9, 10]
    value_fields: [4, 6, 8]
  emigration_pere:
    model: par
    frequency: annual
    time_field: payload[6]
    series_keys: [0, 1, 2, 3, 4, 5]
    value_fields: [7]
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
4. Serializar cada registro sintético al contrato de payload del dataset.
5. Insertar los registros sintéticos en la **misma tabla Raw** del dataset.
6. Registrar la ejecución en `audit.load_log` con `run_type = 'synthetic_projection'`, `model_name`, `synthetic_run_id` y `horizon`.

**Contrato de payload sintético**

| Tipo de dataset | Contrato |
| :--- | :--- |
| Largo | Mismo número y orden de campos que el CSV original; el campo temporal se sustituye por el periodo futuro. |
| Ancho | El registro sintético se serializa en formato largo dentro de `payload` (`series_key`, `period`, `value`) y se marca con `synthetic_layout = 'long'`. Las filas históricas permanecen intactas. |
| Estático | Registro sintético con la misma estructura tabular del CSV; sin periodo futuro. |

> La decisión de serializar los datasets anchos en formato largo evita alterar la estructura histórica del CSV y permite convivir con las filas reales en la misma tabla Raw.

**Marcado de registros sintéticos**

Cada registro insertado tendrá:

```json
{
  "record_origin": "synthetic",
  "synthetic_run_id": "uuid-de-la-ejecucion",
  "synthetic_period": "2027-01",
  "synthetic_model": "PAR",
  "synthetic_layout": "long"
}
```

**Validación**

- El modelo SDV se entrena sin errores y se guarda en disco.
- Se generan los periodos futuros definidos en el horizonte.
- No se sobrescribe ni elimina ningún registro real.
- Los registros sintéticos quedan identificados por `record_origin = 'synthetic'`.
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
```

**Consultas mínimas de proyección sintética**

```sql
-- Registros sintéticos por ejecución
SELECT synthetic_run_id, dataset_name, model_name, horizon, rows_loaded, status
FROM audit.load_log
WHERE run_type = 'synthetic_projection'
ORDER BY started_at DESC;

-- Periodos futuros generados
SELECT dataset_name, synthetic_period, COUNT(*) AS registros
FROM src.foreign_affiliates_gender_raw
WHERE record_origin = 'synthetic'
GROUP BY dataset_name, synthetic_period
ORDER BY synthetic_period;

-- Duplicados de periodo por serie
SELECT synthetic_period, COUNT(*)
FROM src.foreign_affiliates_gender_raw
WHERE record_origin = 'synthetic'
GROUP BY synthetic_period
HAVING COUNT(*) > 1;
```

**Criterios de calidad**

- El SHA-256 de cada CSV coincide con el original.
- El número de filas reales coincide con el número de filas del CSV.
- Las cabeceras originales están completas y sin renombrar.
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
| `time_field` | `TEXT` | Posición o nombre del campo temporal |
| `series_keys` | `JSONB` | Campos que identifican la serie |
| `value_fields` | `JSONB` | Campos numéricos proyectables |
| `loaded_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

### 7.2 `src.<dataset>_raw`

Una tabla por fichero. Estructura genérica y fiel al CSV.

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `row_id` | `BIGSERIAL` | PK |
| `load_id` | `UUID` | NOT NULL |
| `row_number` | `INTEGER` | NOT NULL |
| `payload` | `JSONB` | Array de valores textuales o registro sintético |
| `record_origin` | `VARCHAR(10)` | `real` o `synthetic` |
| `synthetic_run_id` | `UUID` | NULL para registros reales |
| `synthetic_period` | `TEXT` | NULL para registros reales |
| `synthetic_model` | `VARCHAR(40)` | NULL para registros reales |
| `synthetic_layout` | `VARCHAR(10)` | `long` o `wide`, NULL para reales |
| `loaded_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

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

**Por qué `JSONB` y no columnas con los nombres originales**

PostgreSQL limita los identificadores a **63 bytes**. Muchas cabeceras de TGSS superan ampliamente esa longitud y colisionarían o se truncarían, lo que equivaldría a renombrar campos. Guardar cada fila como array `JSONB` y las cabeceras exactas en `src.dataset_files.header_lines` preserva la información original sin truncamientos ni renombrados.

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

> **Importante:** en esta fase no se renombra ninguna columna ni se convierte ningún valor. La columna “Lectura técnica” describe únicamente cómo se abre el fichero.

| Fichero exacto | Encoding | Separador | Filas de cabecera | Tabla Raw destino | Lectura técnica | Proyección temporal |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | `utf-8-sig` | `,` | 1 | `src.cost_of_living_raw` | Carga íntegra; sin mapear `x1..x55` | No aplica (estático) |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` | `latin-1` | `;` | 2 (título + cabecera) | `src.foreign_affiliates_gender_raw` | Carga íntegra; sin normalizar | `PARSynthesizer` mensual |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | `utf-8-sig` | `,` | 1 | `src.eurostat_hitech_employment_raw` | Carga íntegra; sin convertir tipos | `PARSynthesizer` anual |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.epa_labor_market_raw` | Carga íntegra; sin corregir coma decimal | No aplica (estático) |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.istac_employment_cnae_raw` | Carga íntegra; sin `melt` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | `utf-8-sig` | `,` | 1 | `src.istac_employment_divisions_raw` | Carga íntegra; sin `melt` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.istac_employment_status_raw` | Carga íntegra; sin `melt` | `PARSynthesizer` trimestral con extracción larga |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | `utf-8-sig` | `;` | 2 (título + cabecera) | `src.foreign_affiliates_cnae_raw` | Carga íntegra; sin normalizar secciones | `PARSynthesizer` mensual |
| `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` | `utf-8-sig` | `,` | 1 | `src.tech_salaries_raw` | Carga íntegra; sin convertir `salary` ni `salary_currency` | `PARSynthesizer` anual por `work_year` (o modelo tabular si se usa como benchmark estático) |
| `(Datos Externos) Datos de Emigración - INE.csv` | `utf-8-sig` | `;` | 1 | `src.emigration_pere_raw` | Carga íntegra; sin interpretar separador de miles ni celdas `""` | `PARSynthesizer` anual (stock por país/provincia) |

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
4. Los valores sintéticos se serializan al contrato de payload del dataset.
5. Los registros se insertan en la misma tabla Raw con `record_origin = 'synthetic'`.

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
| Fase 4 — dbt | Parsea los payloads, tipa las series, separa reales de sintéticos y calcula los indicadores macroeconómicos del proyecto. |

---

## 11. Criterios de aceptación

- [ ] La carpeta `01_source_system/` existe y contiene los 10 CSV.
- [ ] `docker compose up -d source_db` levanta el servicio en estado `healthy`.
- [ ] Existen los esquemas `src` y `audit`.
- [ ] Existen las 10 tablas Raw, `src.dataset_files` y `audit.load_log`.
- [ ] No existe ningún esquema ni tabla adicional para datos sintéticos.
- [ ] Los 10 CSV se cargan sin errores y con recuentos validados.
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
| Identificadores PostgreSQL de 63 bytes | Cabeceras truncadas o renombradas | Guardar filas en `JSONB` y cabeceras exactas en `src.dataset_files.header_lines` |
| Encoding incorrecto en TGSS | Pérdida de caracteres al leer | Forzar `latin-1` para `3mb...` y `utf-8-sig` para el resto |
| Cabeceras multinivel en EPA/ISTAC | Interpretación errónea de columnas | Almacenar todas las filas y cabeceras; la interpretación se hará en dbt y en la extracción temporal |
| Coma decimal en EPA/ISTAC | Valores alterados si se convierten | No convertir tipos en la carga Raw; la conversión se hará en dbt |
| Series anchas con periodos en columnas | Dificultad para proyectar periodos futuros | Extraer en memoria a formato largo, entrenar PAR y serializar el futuro como registro sintético largo |
| Entrenamiento PAR lento o con alto consumo de memoria | Bloqueo del entorno local | Ajustar épocas, tamaño de lote y horizonte; empezar con un dataset y escalar |
| Fichero PERE de ~4M filas y 333 MB | Carga lenta y alto consumo de memoria | Cargar por lotes con `execute_values`/`COPY`, validar `row_count` y no materializar todo el fichero en memoria |
| Salarios en múltiples monedas | Comparaciones salariales incorrectas | Conservar `salary` y `salary_currency`; usar `salary_in_usd` para comparaciones en dbt |
| Jerarquía geográfica excluyente en PERE | Duplicación del stock al agregar país y provincia | Documentar la jerarquía y filtrar por nivel geográfico en dbt |
| Registros sintéticos confundidos con reales | Análisis incorrectos | Marcar con `record_origin`, `synthetic_run_id`, `synthetic_period` y `synthetic_model` |
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

# Cargar los CSV en crudo, sin transformarlos
python loaders/load_sources_raw.py

# Preparar las series temporales y validar el metadata SDV
python synthetic_data/prepare_own_data.py

# Simular periodos futuros para todos los datasets proyectables
python synthetic_data/simulate_future_raw_data.py --all

# Simular un dataset concreto con horizonte personalizado
python synthetic_data/simulate_future_raw_data.py \
  --dataset foreign_affiliates_gender \
  --horizon 12

# Evaluar la calidad de la proyección sintética
python synthetic_data/evaluate_synthetic.py --all

# Conectarse como lector
psql "host=localhost port=5432 dbname=source_talento user=src_reader password=..."
```

---

## 14. Checklist de ejecución

| Paso | Descripción | Estado |
| :--- | :--- | :--- |
| 1 | Renombrar la fase y preparar el entorno | Pendiente |
| 2 | Diseñar el esquema `src` y el modelo Raw | Pendiente |
| 3 | Crear el contenedor `source_db` y la orquestación | Pendiente |
| 4 | Implementar los scripts SQL de inicialización | Pendiente |
| 5 | Cargar los CSV en crudo sin transformarlos | Pendiente |
| 6 | Preparar las series temporales y los metadatos SDV | Pendiente |
| 7 | Entrenar SDV y simular los registros futuros | Pendiente |
| 8 | Insertar el anexo sintético y validar | Pendiente |
| 9 | Documentar y preparar el handoff a la Fase 2 | Pendiente |

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
