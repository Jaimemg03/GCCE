# Infraestructura BI: Gestión de Fuga y Atracción de Nómadas Digitales en Canarias y España

Proyecto final para la asignatura **Gestión de la Comunicación y Conocimiento Empresarial** (Máster en Ingeniería Informática).

## 🎯 Descripción del Proyecto
Este repositorio implementa un **Modern Data Stack (MDS)** completo y contenerizado para analizar el saldo neto de talento, la retención de profesionales STEM, la brecha salarial frente al coste de vida y el impacto de la llegada de nómadas digitales en Canarias y el conjunto de España.

---

## 🏗️ Arquitectura del Sistema (Fases de Implementación)

El flujo de datos se divide en cuatro capas lógicas y persistentes gestionadas mediante contenedores:

```text
[ PostgreSQL 1 (ERP Simulado + Datos Base) ]
                   │
                   ▼  (Airbyte - Ingesta EL)
[ PostgreSQL 2 (Capa Raw / Landing Zone) ]
                   │
                   ▼  (dbt - Transformaciones)
           [ Modelos Analíticos / Data Marts ]
```

1. **`01_erp_simulado/`**: Contenedor PostgreSQL transaccional que simula el ERP de origen y almacena las tablas de referencia y datos sintéticos.
2. **`02_airbyte_ingesta/`**: Configuración y orquestación de Airbyte para replicar los datos hacia la zona de almacenamiento en crudo, gestionando permisos de lectura y escritura.
3. **`03_raw_database/`**: Contenedor PostgreSQL de destino (*Landing Zone*) con persistencia de datos.
4. **`04_dbt_transformaciones/`**: Proyecto dbt enfocado en la limpieza (`staging`), deduplicación por última versión (`intermediate`) y modelado dimensional (`marts`) para el cálculo de KPIs.

---

## 📊 Tabla de Enlaces y Origen de Ficheros (Capa Raw / Ingesta)

| Nombre exacto del Fichero en Repositorio | Fuente / Organismo | Enlace de Acceso / Descarga | Motivo principal en el proyecto |
| :--- | :--- | :--- | :--- |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según actividad económica CNAE25.csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Analiza de forma localizada los puestos cubiertos en sectores técnicos e ingeniería a nivel regional. |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según divisiones de actividad económica.csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Proporciona series históricas de empleo desglosadas por divisiones de actividad económica en Canarias. |
| `(Datos Internos) Datos Propios del ISTAC - Empleo registrado según situaciones en el empleo.csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Permite evaluar la situación laboral (asalariados, autónomos) dentro del tejido empresarial canario. |
| `(Datos Externos) Encuesta de Poblacion Activa (EPA) - INE  - Ocupados y parados por sexo. Tasas de actividad y de paro.csv` | INE | [INE - Encuesta de Población Activa](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176918&menu=resultados&idp=1254735976595) | Referencia oficial de ocupación, paro y nivel de estudios a nivel estatal. |
| `(Datos Externos) 3mb Afiliados extranjeros por sexo, CCAA, regimen y UEnoUE - Seg Social .csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | Aporta la demografía y volumen bruto de trabajadores extranjeros por CCAA y régimen de cotización. |
| `(Datos Externos ) 5mb25. Afiliados extranjeros por RG y RETA, CCAA, seccion CNAE 25 y UEnoUE - Seg Social (desde ene-26).csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | Permite aislar específicamente el sector TIC (CNAE Sección J) para medir la atracción de talento tecnológico. |
| `(Datos Externos) Datos de Emigración - INE.csv` | INE | [PERE - Tabla 79678 (INE)](https://www.ine.es/jaxiT3/Tabla.htm?L=0&t=79678) | Stock anual de españoles residentes en el extranjero por país, provincia de inscripción, sexo y lugar de nacimiento; proxy de emigración/fuga de talento y saldo neto. |
| `(Datos Externos) Empleo en Alta Tecnología - Eurostat.csv` | Eurostat | [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/view/hrst_st_nocc/default/table) | Mide la ocupación en sectores de alta tecnología para analizar la movilidad en menores de 35 años a escala europea. |
| `(Datos Externos)  costo de vida global -  Kaggle.csv` | Kaggle (Numbeo) | [Enlace en Kaggle](https://www.kaggle.com/datasets/mvieira101/global-cost-of-living) | Permite calcular el **Ratio Salario / Coste de Vida** analizando el impacto de alquileres e inflación local. |
| `(Datos Externos) Salarios Tecnológicos - Kaggle.csv` | Kaggle (Adil Shamim) | [Data Science, AI & ML Job Salaries in 2025](https://www.kaggle.com/datasets/adilshamim8/salaries-for-data-science-jobs) | Benchmark salarial global de perfiles TIC para comparar con España/Canarias y evaluar la brecha salarial real. |

---

## 🚀 Despliegue Rápido
Para levantar la infraestructura base de bases de datos persistentes:
```bash
docker compose up -d
```