# Fase 1: Sistema ERP Simulado y Origen de Datos (PostgreSQL 1)

## 📌 Propósito de la Carpeta
Este directorio contiene todo lo necesario para desplegar el contenedor transaccional primario (`PostgreSQL 1`). Este sistema cumple un doble rol:
1. **Simular un ERP corporativo / plataforma de empleo** mediante tablas relacionales transaccionales (empleados, contratos, ubicaciones, bandas salariales y eventos de movilidad).
2. **Alojar los datos sin tratar / fuentes estáticas** (CSV descargados del INE, Eurostat, ISTAC y TGSS) que servirán como tablas de dimensiones y hechos de referencia.

---

## 📂 Estructura de Directorios Interna
```text
01_erp_simulado/
├── Dockerfile                  # Configuración personalizada de la imagen de PostgreSQL
├── init-scripts/               # Scripts SQL ejecutados automáticamente al inicializar la BD
│   ├── 01_create_tables.sql    # DDL del ERP simulado y tablas de staging
│   └── 02_load_sources.sql     # Ingesta inicial de los CSVs de referencia
├── synthetic_data/             # Scripts en Python para poblar datos transaccionales dinámicos
│   ├── generator.py            # Script usando Faker para generar empleados y flujos
│   └── requirements.txt        # Dependencias de Python (faker, psycopg2, pandas)
└── data_sources/               # Almacenamiento local de los CSVs oficiales descargados
```

---

## 📊 Fuentes de Datos Referenciadas

Todas las rutas son relativas a `01_erp_simulado/data_sources/`. Los nombres de fichero se muestran **exactamente** como están en el repositorio (incluyen espacios y paréntesis), por lo que deben citarse entre comillas en scripts SQL/Python.

| Fichero exacto en el repositorio | Fuente / Organismo | Enlace de acceso / descarga | Formato y particularidades | Granularidad | Motivo principal / KPI |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | Kaggle (Numbeo) | [Global Cost of Living](https://www.kaggle.com/datasets/mvieira101/global-cost-of-living) | CSV ASCII, separador `,`, ~4.956 filas, columnas genéricas `x1..x55` (requieren mapeo a campos Numbeo) | Ciudad / país | **Ratio Salario / Coste de Vida** e impacto inmobiliario |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | CSV **ISO-8859-1**, separador `;`, CRLF, ~3.542 filas, formato ancho | Periodo × CCAA × régimen × sexo × UE/no UE | Demografía y volumen bruto de trabajadores extranjeros |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | Eurostat | [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/view/hrst_st_nocc/default/table) | CSV ASCII, separador `,`, ~66.088 filas, formato largo con flags de observación | Año × país × ocupación ISCO-08 × edad | Movilidad de perfiles de alta tecnología, menores de 35 años |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | INE | [EPA - INE](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176918&menu=resultados&idp=1254735976595) | UTF-8, separador `,`, cabecera multinivel, **coma decimal** | Sexo × ocupados/parados/tasas | Referencia oficial de ocupación y paro en perfiles cualificados |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, cabecera multinivel, ~990 filas | Trimestre × CNAE-25 × cuenta ajena/propia | Puestos cubiertos en sectores técnicos e ingeniería en Canarias |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, formato ancho, 1999–2008 | Trimestre × división de actividad | Series históricas de empleo desglosadas por divisiones |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | ISTAC (Canarias) | [Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | UTF-8, separador `,`, formato ancho, 1999–2026 T2 | Trimestre × situación laboral | Evaluación de asalariados y autónomos en Canarias |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | UTF-8, separador `;`, CRLF, ~181 filas, columnas muy largas | Periodo × CCAA × sección CNAE × régimen × UE/no UE | Aislar el sector TIC (CNAE Sección J/K) para talento tecnológico |

---

## 🔗 Trazabilidad en el Pipeline

```text
01_erp_simulado/data_sources/*.csv
        │  (carga inicial - Fase 1)
        ▼
PostgreSQL 1 (ERP + tablas de referencia)
        │  (Airbyte EL - Fase 2)
        ▼
03_raw_database (Landing Zone, datos en crudo)
        │  (dbt: staging → intermediate → marts - Fase 4)
        ▼
Data Marts / KPIs
```

---

## 🧩 Matriz Fuente → KPI

| KPI | Fuentes implicadas |
| :--- | :--- |
| Tasa de Saldo Neto de Talento | TGSS (`3mb...` y `5mb25...`), EPA, ISTAC |
| Ratio Salario / Coste de Vida | Kaggle (Numbeo) + salarios del ERP simulado |
| Absorción de Perfiles Junior | Eurostat (alta tecnología), EPA, ISTAC CNAE-25 |

---

## ⚠️ Notas Técnicas de Ingestión

- Los CSV de TGSS, EPA e ISTAC usan **formatos anchos y cabeceras multinivel**; se normalizarán en `staging`.
- El fichero `3mb...` requiere decodificación **ISO-8859-1**; el resto está en UTF-8.
- EPA e ISTAC usan **coma decimal**; el tipado se resolverá en dbt.
- En la Landing Zone (`03_raw_database`) los datos se conservarán como texto crudo; la limpieza y el tipado corresponden a dbt.
- Los nombres de fichero contienen espacios (incluidos dobles), paréntesis y espacios antes de la extensión; deben referenciarse entre comillas en scripts de carga.

---

## 🛠️ Pasos de Implementación

1. **Persistencia de Datos:** El contenedor está configurado con un volumen Docker dedicado (`postgres_erp_data`) para asegurar que los datos transaccionales persistan ante reinicios.
2. **Generación de Datos Sintéticos:** Ejecutar el generador en Python para inyectar transacciones simuladas que imiten el comportamiento de la fuga y atracción de talento tecnológico:
   ```bash
   cd synthetic_data
   pip install -r requirements.txt
   python generator.py
   ```
3. **Conexión con Airbyte:** Este contenedor se configurará como la base de datos de **origen** (`Source`) en la Fase 2 para que Airbyte extraiga los cambios incrementales hacia la capa `Raw`.