# Plan de Implementación — Fase 1: ERP Simulado, Carga Raw y Datos Sintéticos SDV

> **Proyecto:** Gestión de Fuga y Atracción de Nómadas Digitales Cualificados en Canarias y España  
> **Fase:** 1 de 4 — `01_erp_simulado`  
> **Componente:** PostgreSQL 1 (sistema transaccional de origen)  
> **Estado:** Planificado (v2 — sin transformación de CSV en esta fase)  
> **Última actualización:** 2026-11-10

---

## 0. Principio rector: los CSV no se transforman en la Fase 1

Esta fase es **exclusivamente de origen y carga**. Por tanto:

- Los 8 ficheros de `data_sources/` son **inmutables**: no se editan, recortan, renombran ni sobrescriben.
- **No se renombran columnas** ni se traducen cabeceras.
- **No se cambian tipos de datos**: todos los valores se conservan como texto.
- **No se hace `melt`, `pivot`, limpieza, normalización, deduplicación ni imputación**.
- **No se corrigen comas decimales, encodings de contenido ni formatos de fecha**.
- Cualquier transformación de negocio se realizará en la **Fase 4 (dbt)**, sobre la copia Raw.

La Fase 1 solo aplica la **lectura técnica mínima** necesaria para poder cargar un CSV en PostgreSQL: detectar encoding, delimitador y número de filas de cabecera. Nada más.

---

## 1. Objetivo de la fase

Construir el sistema de origen del Modern Data Stack: un PostgreSQL contenerizado que cumpla dos funciones:

1. **Simular un ERP/CRM de talento** con tablas relacionales de empleados, contratos, ubicaciones, bandas salariales y eventos de movilidad.
2. **Almacenar los datos base de los CSV tal como llegan**, en un esquema Raw de origen, preservando ficheros, filas, nombres de campos y valores originales, para que Airbyte los replique en la Fase 2 hacia la Landing Zone.

Además, la fase genera **nuevos datos sintéticos** para el ERP mediante la librería **SDV (Synthetic Data Vault)**, usando uno o varios de sus mecanismos: Gaussian Copula, CTGAN, PAR, HMA o “Use Your Own Data”.

Al finalizar, el contenedor debe estar operativo, poblado, validado y listo para configurarse como `Source` en Airbyte.

---

## 2. Alcance

### Incluido

- Diseño del esquema relacional del ERP simulado.
- Creación del contenedor PostgreSQL 1 y su volumen persistente.
- Scripts SQL de esquemas, tablas, restricciones, índices y catálogos estáticos del ERP.
- **Carga Raw e íntegra de los 8 CSV** en el esquema `src`, sin transformarlos.
- Preservación de los nombres originales de campo y de los valores originales.
- Generación de datos sintéticos con **SDV** para las tablas del ERP.
- Metadatos SDV, entrenamiento, muestreo y evaluación de calidad.
- Roles de base de datos separados por función (administración, escritura, lectura).
- Validaciones de integridad, calidad y fidelidad de la carga.
- Documentación de ejecución y handoff a la Fase 2.

### No incluido

- Renombrar, tipar, limpiar, normalizar o deduplicar los CSV.
- Traducir cabeceras o valores.
- Resolver comas decimales, periodos trimestrales o cabeceras multinivel.
- Configuración de conectores Airbyte (Fase 2).
- Creación de la base de datos Raw de destino (Fase 3).
- Modelos dbt y KPIs finales (Fase 4).
- Cuadros de mando o visualización.

---

## 3. Estado inicial del repositorio

A fecha de este plan:

```text
01_erp_simulado/
├── README.md
├── data_sources/          # 8 CSV reales ya descargados
├── init-scripts/          # vacío
└── synthetic_data/        # vacío
```

No existe todavía `docker-compose.yml`, `Dockerfile`, scripts SQL ni generador. Los CSV están disponibles y sus particularidades técnicas ya están documentadas en el `README.md` de la fase.

---

## 4. Resultado esperado al finalizar la fase

```text
01_erp_simulado/
├── Dockerfile
├── .env.example
├── README.md
├── PLAN_IMPLEMENTACION_FASE_1.md
├── init-scripts/
│   ├── 00_bootstrap_roles.sh
│   ├── 01_create_schemas.sql
│   ├── 02_create_tables.sql
│   ├── 03_indexes_constraints.sql
│   ├── 04_seed_static_data.sql
│   └── 05_grants.sql
├── loaders/
│   ├── load_sources_raw.py
│   └── requirements.txt
├── synthetic_data/
│   ├── metadata.json
│   ├── config.yaml
│   ├── prepare_own_data.py
│   ├── train_synthesizer.py
│   ├── generate_synthetic.py
│   ├── evaluate_synthetic.py
│   ├── models/              # artefactos SDV entrenados
│   ├── seed/                # seed relacional mínimo versionado
│   └── requirements.txt
└── data_sources/
    └── (8 CSV de referencia, sin modificar)
```

Además, en la raíz del repositorio se añadirá el servicio `erp_db` dentro de `docker-compose.yml`, dejando preparada la incorporación futura de Airbyte y PostgreSQL 2.

---

## 5. Decisiones de diseño

| Decisión | Justificación |
| :--- | :--- |
| No transformar los CSV en Fase 1 | La arquitectura exige que la limpieza y el modelado ocurran en dbt (Fase 4). |
| Guardar cada fila CSV como array `JSONB` de valores textuales | Evita renombrar campos y conserva valores originales; PostgreSQL trunca identificadores a 63 bytes, por lo que crear columnas con las cabeceras reales sería inviable. |
| Guardar las cabeceras originales en `src.dataset_files.header_lines` | Preserva los nombres de campo exactos, incluidas cabeceras multinivel. |
| Cargar todas las filas, incluidas cabeceras y líneas vacías | Máxima fidelidad respecto al fichero original; dbt decidirá qué descartar. |
| Usar `src.<dataset>_raw` con `row_number` | Permite reconstruir el orden original y auditar la carga. |
| Calcular SHA-256 de cada CSV | Verifica que el fichero no ha sido modificado y hace la carga idempotente. |
| Generar datos sintéticos con SDV | Sustituye Faker por síntesis estadística/neuronal reproducible y evaluable. |
| HMA como mecanismo principal | Modela tablas relacionadas preservando claves primarias y foráneas del ERP. |
| Gaussian Copula como baseline | Rápido, transparente y útil para comparar calidad y tiempos. |
| CTGAN y PAR como mecanismos opcionales | CTGAN para alta fidelidad; PAR para secuencias de eventos de movilidad. |
| Roles separados `erp_admin`, `erp_writer`, `erp_reader` | Aplica el principio de mínimo privilegio exigido por la arquitectura. |
| Volumen Docker dedicado | Persistencia de datos entre reinicios. |

---

## 6. Pasos de implementación

### Paso 1 — Preparar entorno y convenciones

**Qué hacer**

1. Verificar que Docker y Docker Compose están disponibles.
2. Crear `.env.example` con las variables de conexión y contraseñas de roles.
3. Definir convenciones:
   - Nombres de tablas en `snake_case`.
   - Claves primarias con sufijo `_id`.
   - Fechas del ERP en `DATE`, timestamps en `TIMESTAMPTZ`.
   - Importes del ERP en `NUMERIC(12,2)` y moneda en `CHAR(3)`.
   - Todo registro del ERP con `created_at` y `updated_at`.
4. Añadir `.env` al `.gitignore`.

**Archivos**

- `.env.example`
- `.gitignore` (raíz)

**Variables previstas**

```dotenv
POSTGRES_DB=erp_talento
POSTGRES_USER=erp_admin
POSTGRES_PASSWORD=cambiar_en_local
ERP_WRITER_USER=erp_writer
ERP_WRITER_PASSWORD=cambiar_en_local
ERP_READER_USER=erp_reader
ERP_READER_PASSWORD=cambiar_en_local
```

**Validación**

- `docker --version` y `docker compose version` responden correctamente.
- `.env.example` no contiene secretos reales.

---

### Paso 2 — Diseñar esquemas y modelo de datos

**Qué hacer**

1. Crear el esquema `erp` con las entidades transaccionales simuladas.
2. Crear el esquema `src` para los CSV cargados en crudo, sin transformar.
3. Crear el esquema `audit` para registrar cargas y ejecuciones.
4. Definir claves primarias, claves foráneas, `CHECK` e índices del ERP.
5. Crear las tablas Raw de `src` con estructura genérica y fiel.

**Entidades del ERP**

| Tabla | Propósito | Campos clave |
| :--- | :--- | :--- |
| `erp.locations` | Ciudades y países de origen/destino | `location_id`, `city`, `country_code`, `region`, `is_canary`, `cost_of_living_index` |
| `erp.job_families` | Familias profesionales y clasificación | `job_family_id`, `name`, `isco_code`, `cnae_section`, `is_stem`, `is_tech` |
| `erp.salary_bands` | Bandas salariales por familia y seniority | `salary_band_id`, `job_family_id`, `seniority`, `min_salary`, `max_salary` |
| `erp.employees` | Perfiles profesionales simulados | `employee_id`, `nationality`, `education_level`, `job_family_id`, `location_id`, `hire_date`, `exit_date`, `status`, `is_digital_nomad` |
| `erp.contracts` | Contratos y condiciones económicas | `contract_id`, `employee_id`, `contract_type`, `start_date`, `end_date`, `annual_salary`, `work_mode`, `salary_band_id` |
| `erp.mobility_events` | Flujos de entrada, salida y movilidad interna | `event_id`, `employee_id`, `event_type`, `event_date`, `origin_location_id`, `destination_location_id`, `reason_code` |

**Tablas Raw de fuentes**

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

**Trazabilidad**

- `src.dataset_files`: metadatos de cada fichero cargado.
- `audit.load_log`: resultado de cada ejecución de carga.

**Validación**

- Todas las tablas existen en los esquemas correctos.
- Las claves foráneas del ERP impiden registros huérfanos.
- Los `CHECK` validan salarios positivos, fechas coherentes y estados permitidos.
- Las tablas Raw no contienen columnas renombradas ni valores transformados.

---

### Paso 3 — Crear contenedor PostgreSQL y orquestación

**Qué hacer**

1. Crear `01_erp_simulado/Dockerfile` basado en `postgres:16-alpine`.
2. Copiar los scripts de inicialización a `/docker-entrypoint-initdb.d`.
3. Declarar en `docker-compose.yml` el servicio `erp_db` con:
   - Variables desde `.env`.
   - Volumen `postgres_erp_data`.
   - Montaje de `data_sources` en modo **solo lectura**.
   - `healthcheck` con `pg_isready`.
   - Red compartida para futuras fases.
4. Exponer el puerto `5432` solo en local.

**Archivos**

- `01_erp_simulado/Dockerfile`
- `docker-compose.yml` (raíz)

**Validación**

- `docker compose up -d` levanta el servicio en estado `healthy`.
- `docker compose logs erp_db` no muestra errores de inicialización.
- El volumen persiste tras `docker compose restart`.
- El montaje de `data_sources` es de solo lectura.

---

### Paso 4 — Implementar scripts de inicialización SQL

**Qué hacer**

1. `00_bootstrap_roles.sh`: crear roles y contraseñas desde variables de entorno.
2. `01_create_schemas.sql`: crear `erp`, `src` y `audit`.
3. `02_create_tables.sql`: DDL del ERP y de las tablas Raw.
4. `03_indexes_constraints.sql`: índices, claves foráneas y `CHECK`.
5. `04_seed_static_data.sql`: catálogos estáticos del ERP.
6. `05_grants.sql`: permisos por rol.

**Datos estáticos a sembrar en el ERP**

- Países y regiones relevantes (España, Canarias, UE, extra-UE).
- Familias profesionales STEM/TIC.
- Códigos ISCO y secciones CNAE necesarios.
- Bandas salariales base por seniority.
- Catálogo de motivos de movilidad.

> Estos catálogos pertenecen al ERP simulado y no modifican los CSV.

**Orden de ejecución**

Los scripts se ejecutan alfabéticamente por el entrypoint de PostgreSQL. Por eso el prefijo numérico es obligatorio.

**Validación**

- `\dn` muestra los tres esquemas.
- `\dt erp.*`, `\dt src.*` y `\dt audit.*` listan las tablas esperadas.
- `SELECT * FROM erp.job_families;` devuelve datos.
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
6. Registrar la carga en `audit.load_log`.
7. Hacer la carga idempotente por `file_hash`: relanzar el script no debe duplicar filas.

**Qué NO hace el loader**

- No renombra columnas.
- No convierte tipos.
- No elimina filas.
- No traduce valores.
- No normaliza formatos.
- No hace `melt`, `pivot`, `join` ni deduplicación.
- No escribe nunca en `data_sources/`.

**Ejemplo conceptual de fila Raw**

```json
{
  "row_number": 4,
  "payload": ["202601", "TOTAL", "2644426.35", "838003.65", "1806422.70"]
}
```

Los nombres de campo originales se conservan en `src.dataset_files.header_lines`, por ejemplo:

```json
[
  ["Período", "CCAA", "TOTAL RG Y RETA TOTAL REGIMEN TOTAL UE Y NO UE", "..."]
]
```

**Validación**

- El SHA-256 calculado coincide con el del fichero original.
- El número de filas cargadas coincide con el número de filas leídas del CSV.
- Las cabeceras originales están íntegras en `src.dataset_files.header_lines`.
- No existe ninguna columna renombrada ni valor transformado.
- `audit.load_log` refleja `status = 'success'` para los 8 ficheros.
- Relanzar el loader no duplica registros.

---

### Paso 6 — Generar datos sintéticos con SDV

**Qué hacer**

1. Crear `synthetic_data/metadata.json` con la descripción de las tablas del ERP: columnas, tipos SDV, claves primarias, claves foráneas y relaciones.
2. Crear `synthetic_data/config.yaml` con:
   - Mecanismo SDV seleccionado.
   - Semilla.
   - Épocas y tamaño de lote.
   - Volúmenes objetivo.
3. Crear `synthetic_data/seed/` con un **seed relacional mínimo y versionado** del ERP:
   - `locations.csv`
   - `job_families.csv`
   - `salary_bands.csv`
   - `employees.csv`
   - `contracts.csv`
   - `mobility_events.csv`
4. Crear `synthetic_data/prepare_own_data.py` para cargar el seed y validar el metadata (“Use Your Own Data”).
5. Crear `synthetic_data/train_synthesizer.py` para entrenar el mecanismo elegido y guardar el modelo en `synthetic_data/models/`.
6. Crear `synthetic_data/generate_synthetic.py` para muestrear los volúmenes objetivo e insertarlos en `erp.*`.
7. Crear `synthetic_data/evaluate_synthetic.py` para medir calidad, similitud y privacidad.

**Mecanismos SDV y uso previsto**

| Mecanismo SDV | Clase / flujo | Uso en esta fase |
| :--- | :--- | :--- |
| **Synthesize Multi Tables (HMA)** | `HMASynthesizer` | **Principal**: genera las tablas relacionales del ERP preservando PK/FK. |
| **Synthesize a Table (Gaussian Copula)** | `GaussianCopulaSynthesizer` | Baseline rápido y transparente para tablas individuales. |
| **Synthesize a Table (CTGAN)** | `CTGANSynthesizer` | Opcional para alta fidelidad en tablas con distribuciones complejas. |
| **Synthesize Sequences (PAR)** | `ParSynthesizer` | Opcional para secuencias temporales de `mobility_events`. |
| **Use Your Own Data** | Carga de seed + `metadata.json` | Obligatorio para preparar el seed y describir el esquema antes de entrenar. |

**Estrategia recomendada**

1. Usar **Use Your Own Data** para cargar el seed y escribir/validar `metadata.json`.
2. Entrenar **HMA** sobre el seed relacional y generar las tablas del ERP.
3. Usar **Gaussian Copula** como baseline de comparación de calidad y tiempos.
4. Si la calidad de alguna tabla individual no es suficiente, probar **CTGAN**.
5. Si se modelan secuencias de eventos, probar **PAR** sobre `mobility_events`.
6. Guardar el modelo, la semilla y el metadata para reproducibilidad.

**Volúmenes orientativos**

| Entidad | Seed | Salida sintética |
| :--- | :--- | :--- |
| `erp.locations` | 20 | 30–50 |
| `erp.job_families` | 10 | 10–15 |
| `erp.salary_bands` | 40 | 40–60 |
| `erp.employees` | 200 | 5.000 |
| `erp.contracts` | 300 | 6.500 |
| `erp.mobility_events` | 1.000 | 20.000 |

**Metadata SDV (ejemplo resumido)**

```json
{
  "tables": {
    "employees": {
      "primary_key": "employee_id",
      "columns": {
        "employee_id": { "sdtype": "id" },
        "job_family_id": { "sdtype": "id" },
        "location_id": { "sdtype": "id" },
        "hire_date": { "sdtype": "datetime", "datetime_format": "%Y-%m-%d" },
        "annual_salary": { "sdtype": "numerical" },
        "status": { "sdtype": "categorical" }
      }
    },
    "contracts": {
      "primary_key": "contract_id",
      "columns": {
        "contract_id": { "sdtype": "id" },
        "employee_id": { "sdtype": "id" },
        "annual_salary": { "sdtype": "numerical" }
      }
    }
  },
  "relationships": [
    {
      "parent_table_name": "employees",
      "parent_primary_key": "employee_id",
      "child_table_name": "contracts",
      "child_foreign_key": "employee_id"
    }
  ]
}
```

**Lógica de negocio que debe emerger del seed**

- Mezcla de perfiles nacionales, UE y extra-UE.
- Mayor peso de perfiles tecnológicos y STEM.
- Contratos indefinidos, temporales, freelance y remotos.
- Eventos de alta, salida, llegada internacional, salida internacional, promoción y cambio salarial.
- Salarios coherentes con banda, seniority y coste de vida de la ubicación.
- Fechas dentro del periodo 2019–2026 y posteriores a la fecha de alta.

**Validación**

- El modelo SDV se entrena sin errores y se guarda en disco.
- El metadata valida correctamente con SDV.
- Se generan los volúmenes objetivo.
- No hay claves foráneas huérfanas en las tablas sintéticas.
- Las distribuciones sintéticas son razonablemente similares a las del seed.
- El proceso es reproducible con la misma semilla y el mismo modelo.

---

### Paso 7 — Configurar roles y permisos

**Qué hacer**

1. Crear `erp_admin` como propietario lógico.
2. Crear `erp_writer` con permisos de `INSERT`, `UPDATE`, `DELETE` y `SELECT` sobre `erp`, `src` y `audit`.
3. Crear `erp_reader` con solo `SELECT` sobre `erp` y `src`.
4. Revocar permisos por defecto no necesarios.
5. Documentar las credenciales de conexión para Airbyte.

**Validación**

- `erp_reader` puede leer pero no escribir.
- `erp_writer` puede ejecutar el loader y el generador SDV.
- Ningún rol usa la cuenta superusuario en la Fase 2.

---

### Paso 8 — Validar integridad, fidelidad y calidad

**Qué hacer**

1. Validar que los CSV no han sido modificados.
2. Validar la carga Raw.
3. Validar la integridad del ERP.
4. Validar la calidad de los datos sintéticos.

**Consultas mínimas de carga Raw**

```sql
-- Ficheros cargados y hashes
SELECT dataset_name, source_file, row_count, sha256, loaded_at
FROM src.dataset_files
ORDER BY dataset_name;

-- Filas cargadas por dataset
SELECT dataset_name, COUNT(*) AS filas
FROM src.foreign_affiliates_gender_raw
GROUP BY dataset_name;

-- Cabeceras originales preservadas
SELECT dataset_name, header_lines
FROM src.dataset_files
WHERE dataset_name = 'foreign_affiliates_gender';

-- Estado de cargas
SELECT source_file, rows_loaded, status, started_at, finished_at
FROM audit.load_log
ORDER BY started_at DESC;
```

**Consultas mínimas del ERP**

```sql
SELECT 'employees' AS tabla, COUNT(*) FROM erp.employees
UNION ALL SELECT 'contracts', COUNT(*) FROM erp.contracts
UNION ALL SELECT 'mobility_events', COUNT(*) FROM erp.mobility_events;

SELECT COUNT(*) FROM erp.employees e
LEFT JOIN erp.contracts c ON c.employee_id = e.employee_id
WHERE c.contract_id IS NULL;

SELECT COUNT(*) FROM erp.employees WHERE exit_date < hire_date;

SELECT COUNT(*) FROM erp.contracts WHERE annual_salary <= 0;
```

**Criterios de calidad**

- El SHA-256 de cada CSV coincide con el original.
- El número de filas Raw coincide con el número de filas del CSV.
- Las cabeceras originales están completas y sin renombrar.
- No hay huérfanos en claves foráneas del ERP.
- No hay fechas imposibles ni salarios nulos o negativos.
- Los datos sintéticos superan una evaluación básica de similitud y no contienen datos reales.

---

### Paso 9 — Documentar y preparar handoff a la Fase 2

**Qué hacer**

1. Actualizar el `README.md` de la fase con:
   - Comandos definitivos de arranque.
   - Estructura final de directorios.
   - Descripción de esquemas y tablas.
   - Mecanismo SDV elegido y comandos de entrenamiento/generación.
   - Credenciales de ejemplo y rol recomendado para Airbyte.
2. Añadir un apartado de troubleshooting:
   - Reinicializar volumen.
   - Reejecutar loader Raw.
   - Reentrenar o regenerar datos sintéticos.
3. Documentar el contrato de datos para Airbyte:
   - Esquemas a replicar: `erp` y `src`.
   - Tablas con clave primaria.
   - Campos de cursor incrementales: `updated_at` en el ERP.
   - Tablas Raw sin transformar.
4. Entregar a la Fase 2:
   - Host, puerto, base de datos.
   - Usuario `erp_reader` y contraseña.
   - Lista de tablas y volúmenes esperados.

**Validación**

- Un tercero puede levantar la fase desde cero siguiendo el README.
- Airbyte puede conectarse con `erp_reader` y descubrir `erp` y `src`.
- Los volúmenes documentados coinciden con la realidad.

---

## 7. Modelo de datos detallado

### 7.1 Esquema `erp` (datos sintéticos del ERP)

#### `erp.locations`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `location_id` | `SERIAL` | PK |
| `city` | `VARCHAR(120)` | NOT NULL |
| `country_code` | `CHAR(2)` | NOT NULL |
| `country_name` | `VARCHAR(120)` | NOT NULL |
| `region` | `VARCHAR(40)` | `Canarias`, `España`, `UE`, `Extra-UE` |
| `is_canary` | `BOOLEAN` | DEFAULT `FALSE` |
| `cost_of_living_index` | `NUMERIC(8,2)` | NULL permitido |
| `created_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |
| `updated_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

#### `erp.job_families`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `job_family_id` | `SERIAL` | PK |
| `name` | `VARCHAR(120)` | UNIQUE, NOT NULL |
| `isco_code` | `VARCHAR(10)` | NULL permitido |
| `cnae_section` | `CHAR(1)` | NULL permitido |
| `is_stem` | `BOOLEAN` | DEFAULT `FALSE` |
| `is_tech` | `BOOLEAN` | DEFAULT `FALSE` |

#### `erp.salary_bands`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `salary_band_id` | `SERIAL` | PK |
| `job_family_id` | `INTEGER` | FK → `erp.job_families` |
| `seniority` | `VARCHAR(20)` | `Junior`, `Mid`, `Senior`, `Lead` |
| `min_salary` | `NUMERIC(12,2)` | `> 0` |
| `max_salary` | `NUMERIC(12,2)` | `>= min_salary` |
| `currency` | `CHAR(3)` | DEFAULT `'EUR'` |

#### `erp.employees`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `employee_id` | `UUID` | PK |
| `first_name` | `VARCHAR(80)` | NOT NULL |
| `last_name` | `VARCHAR(120)` | NOT NULL |
| `birth_date` | `DATE` | NOT NULL |
| `gender` | `VARCHAR(20)` | NOT NULL |
| `nationality` | `VARCHAR(80)` | NOT NULL |
| `country_of_origin` | `CHAR(2)` | NOT NULL |
| `education_level` | `VARCHAR(40)` | NOT NULL |
| `job_family_id` | `INTEGER` | FK → `erp.job_families` |
| `location_id` | `INTEGER` | FK → `erp.locations` |
| `hire_date` | `DATE` | NOT NULL |
| `exit_date` | `DATE` | NULL |
| `status` | `VARCHAR(20)` | `active`, `exited`, `remote`, `relocated` |
| `is_digital_nomad` | `BOOLEAN` | DEFAULT `FALSE` |
| `source_version` | `INTEGER` | DEFAULT `1` |
| `created_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |
| `updated_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

#### `erp.contracts`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `contract_id` | `SERIAL` | PK |
| `employee_id` | `UUID` | FK → `erp.employees` |
| `contract_type` | `VARCHAR(40)` | NOT NULL |
| `start_date` | `DATE` | NOT NULL |
| `end_date` | `DATE` | NULL |
| `annual_salary` | `NUMERIC(12,2)` | `> 0` |
| `currency` | `CHAR(3)` | DEFAULT `'EUR'` |
| `work_mode` | `VARCHAR(20)` | `onsite`, `hybrid`, `remote` |
| `salary_band_id` | `INTEGER` | FK → `erp.salary_bands` |
| `source_version` | `INTEGER` | DEFAULT `1` |
| `created_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |
| `updated_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

#### `erp.mobility_events`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `event_id` | `BIGSERIAL` | PK |
| `employee_id` | `UUID` | FK → `erp.employees` |
| `event_type` | `VARCHAR(40)` | NOT NULL |
| `event_date` | `DATE` | NOT NULL |
| `origin_location_id` | `INTEGER` | FK → `erp.locations`, NULL permitido |
| `destination_location_id` | `INTEGER` | FK → `erp.locations`, NULL permitido |
| `reason_code` | `VARCHAR(40)` | NOT NULL |
| `previous_salary` | `NUMERIC(12,2)` | NULL permitido |
| `new_salary` | `NUMERIC(12,2)` | NULL permitido |
| `source_version` | `INTEGER` | DEFAULT `1` |
| `created_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |
| `updated_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

---

### 7.2 Esquema `src` (CSV cargados en crudo)

#### `src.dataset_files`

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
| `loaded_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

#### `src.<dataset>_raw`

Una tabla por fichero. Estructura genérica y fiel al CSV.

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `row_id` | `BIGSERIAL` | PK |
| `load_id` | `UUID` | NOT NULL |
| `row_number` | `INTEGER` | NOT NULL |
| `payload` | `JSONB` | Array de valores textuales |
| `loaded_at` | `TIMESTAMPTZ` | DEFAULT `NOW()` |

Restricción de unicidad:

```sql
UNIQUE (load_id, row_number)
```

**Por qué `JSONB` y no columnas con los nombres originales**

PostgreSQL limita los identificadores a **63 bytes**. Muchas cabeceras de TGSS superan ampliamente esa longitud y colisionarían o se truncarían, lo que equivaldría a renombrar campos. Guardar cada fila como array `JSONB` y las cabeceras exactas en `src.dataset_files.header_lines` preserva la información original sin truncamientos ni renombrados.

---

### 7.3 Esquema `audit`

#### `audit.load_log`

| Columna | Tipo | Restricción |
| :--- | :--- | :--- |
| `load_id` | `UUID` | PK |
| `dataset_name` | `VARCHAR(80)` | NOT NULL |
| `source_file` | `TEXT` | NOT NULL |
| `file_hash` | `CHAR(64)` | NOT NULL |
| `rows_loaded` | `INTEGER` | NOT NULL |
| `status` | `VARCHAR(20)` | `success`, `failed` |
| `started_at` | `TIMESTAMPTZ` | NOT NULL |
| `finished_at` | `TIMESTAMPTZ` | NULL |
| `error_message` | `TEXT` | NULL |

---

## 8. Mapeo de fuentes CSV a tablas Raw

> **Importante:** en esta fase no se renombra ninguna columna ni se convierte ningún valor. La columna “Nota” describe únicamente la lectura técnica.

| Fichero exacto | Encoding | Separador | Filas de cabecera | Tabla Raw destino | Nota |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | `utf-8-sig` | `,` | 1 | `src.cost_of_living_raw` | Carga íntegra; sin mapear `x1..x55` |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` | `latin-1` | `;` | 2 (título + cabecera) | `src.foreign_affiliates_gender_raw` | Carga íntegra; sin normalizar a formato largo |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | `utf-8-sig` | `,` | 1 | `src.eurostat_hitech_employment_raw` | Carga íntegra; sin convertir tipos |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.epa_labor_market_raw` | Carga íntegra; sin corregir coma decimal |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.istac_employment_cnae_raw` | Carga íntegra; sin `melt` |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | `utf-8-sig` | `,` | 1 | `src.istac_employment_divisions_raw` | Carga íntegra; sin `melt` |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | `utf-8-sig` | `,` | 2 (cabecera multinivel) | `src.istac_employment_status_raw` | Carga íntegra; sin `melt` |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | `utf-8-sig` | `;` | 2 (título + cabecera) | `src.foreign_affiliates_cnae_raw` | Carga íntegra; sin normalizar secciones |

---

## 9. KPIs que prepara esta fase

Los KPIs **no se calculan en esta fase**. La Fase 1 solo deja disponibles los datos de origen y los datos sintéticos necesarios para que dbt los calcule en la Fase 4.

| KPI | Datos de la Fase 1 | Cálculo futuro en Fase 4 |
| :--- | :--- | :--- |
| Tasa de Saldo Neto de Talento | `erp.mobility_events`, `erp.employees` | Entradas internacionales menos salidas sobre plantilla media |
| Ratio Salario / Coste de Vida | `erp.contracts`, `erp.locations`, `src.cost_of_living_raw` | Salario anual ajustado por índice de coste de vida |
| Absorción de Perfiles Junior | `erp.contracts`, `erp.employees`, `src.eurostat_hitech_employment_raw`, `src.epa_labor_market_raw` | Contrataciones junior sobre total de contrataciones |

---

## 10. Criterios de aceptación

- [ ] `docker compose up -d` levanta `erp_db` en estado `healthy`.
- [ ] Existen los esquemas `erp`, `src` y `audit`.
- [ ] Todas las tablas y restricciones están creadas.
- [ ] Los 8 CSV se cargan sin errores y con recuentos validados.
- [ ] El SHA-256 de cada CSV coincide con el del fichero original.
- [ ] Las cabeceras originales están preservadas en `src.dataset_files.header_lines`.
- [ ] Ningún CSV ha sido modificado, renombrado ni transformado.
- [ ] El modelo SDV se entrena y se guarda correctamente.
- [ ] Se generan al menos 5.000 empleados, 6.500 contratos y 20.000 eventos sintéticos.
- [ ] No hay claves foráneas huérfanas ni duplicados en claves naturales del ERP.
- [ ] `erp_reader` solo puede leer; `erp_writer` puede escribir.
- [ ] El volumen persiste tras reiniciar el contenedor.
- [ ] El README de la fase permite reproducir todo desde cero.
- [ ] Airbyte puede conectarse con `erp_reader` y descubrir `erp` y `src`.

---

## 11. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
| :--- | :--- | :--- |
| Identificadores PostgreSQL de 63 bytes | Cabeceras truncadas o renombradas | Guardar filas en `JSONB` y cabeceras exactas en `src.dataset_files.header_lines` |
| Encoding incorrecto en TGSS | Pérdida de caracteres al leer | Forzar `latin-1` para `3mb...` y `utf-8-sig` para el resto |
| Cabeceras multinivel en EPA/ISTAC | Interpretación errónea de columnas | Almacenar todas las filas y cabeceras; la interpretación se hará en dbt |
| Coma decimal en EPA/ISTAC | Valores alterados si se convierten | No convertir tipos en Fase 1; conservar texto original |
| Los scripts de init solo corren con volumen vacío | Cambios no aplicados | Documentar `docker compose down -v` para reinicializar |
| Nombres de fichero con espacios | Errores de apertura | Rutas entre comillas y resolución con `pathlib` |
| Entrenamiento SDV lento o con alto consumo de memoria | Bloqueo del entorno local | Empezar con Gaussian Copula/HMA y volúmenes moderados; escalar después |
| Metadata HMA mal definido | Violaciones de integridad referencial | Validar `metadata.json` antes de entrenar y comprobar PK/FK tras el muestreo |
| Fuga de datos reales en el sintético | Riesgo de privacidad | Usar SDV y evaluar similitud/privacidad; no entrenar con datos personales reales |
| Airbyte no detecta cambios incrementales | Duplicados o pérdida de datos | Incluir `updated_at` y clave primaria en todas las tablas del ERP |
| Credenciales en el repositorio | Riesgo de seguridad | `.env` fuera de Git y `.env.example` sin secretos |

---

## 12. Comandos de referencia

```bash
# Levantar PostgreSQL 1
docker compose up -d erp_db

# Ver estado y logs
docker compose ps
docker compose logs -f erp_db

# Reinicializar desde cero (borra el volumen)
docker compose down -v
docker compose up -d erp_db

# Cargar los CSV en crudo, sin transformarlos
python loaders/load_sources_raw.py

# Preparar el seed y validar el metadata SDV
python synthetic_data/prepare_own_data.py

# Entrenar el modelo SDV principal (HMA)
python synthetic_data/train_synthesizer.py --model hma

# Entrenar alternativas de comparación
python synthetic_data/train_synthesizer.py --model gaussian_copula
python synthetic_data/train_synthesizer.py --model ctgan
python synthetic_data/train_synthesizer.py --model par

# Generar los datos sintéticos del ERP
python synthetic_data/generate_synthetic.py --model hma

# Evaluar calidad y similitud del dato sintético
python synthetic_data/evaluate_synthetic.py --model hma

# Conectarse como lector
psql "host=localhost port=5432 dbname=erp_talento user=erp_reader password=..."
```

---

## 13. Checklist de ejecución

| Paso | Descripción | Estado |
| :--- | :--- | :--- |
| 1 | Preparar entorno y convenciones | Pendiente |
| 2 | Diseñar esquemas y modelo de datos | Pendiente |
| 3 | Crear contenedor y orquestación | Pendiente |
| 4 | Implementar scripts SQL de inicialización | Pendiente |
| 5 | Cargar CSV en crudo sin transformarlos | Pendiente |
| 6 | Generar datos sintéticos con SDV | Pendiente |
| 7 | Configurar roles y permisos | Pendiente |
| 8 | Validar integridad, fidelidad y calidad | Pendiente |
| 9 | Documentar y preparar handoff a Fase 2 | Pendiente |

---

## 14. Handoff a la Fase 2

Al cerrar esta fase se entregará:

- Contenedor `erp_db` operativo y persistente.
- Base de datos `erp_talento` con esquemas `erp`, `src` y `audit`.
- Tablas Raw `src.*` con los CSV íntegros, sin transformar.
- Tablas `erp.*` pobladas con datos sintéticos generados por SDV.
- Metadatos, configuración, seed y modelos SDV versionados.
- Usuario `erp_reader` con permisos de solo lectura para Airbyte.
- Inventario de tablas, claves primarias y campos `updated_at`.
- Volúmenes esperados por tabla.
- README actualizado con instrucciones reproducibles.
- Este plan marcado con el estado real de ejecución.
