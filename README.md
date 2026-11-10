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

## 📊 Tabla de Enlaces y Origen de Ficheros

| Nombre del Fichero en Repositorio | Fuente / Organismo | Enlace de Acceso / Descarga | Motivo principal en el proyecto |
| :--- | :--- | :--- | :--- |
| `costo de vida global - Kaggle.csv` | Kaggle (Numbeo) | [Enlace en Kaggle](https://www.kaggle.com/datasets/mvieira101/global-cost-of-living) | Permite calcular el **Ratio Salario / Coste de Vida** analizando el impacto inmobiliario. |
| `3mb Afiliados extranjeros... - Seg Social.csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | Demografía y volumen bruto de trabajadores extranjeros por CCAA y régimen. |
| `Empleo en Alta Tecnología - Eurostat.csv` | Eurostat | [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/view/hrst_st_nocc/default/table) | Ocupación en alta tecnología para analizar la movilidad en menores de 35 años. |
| `Encuesta de Poblacion Activa (EPA) - INE...csv` | INE | [INE - Encuesta de Población Activa](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176918&menu=resultados&idp=1254735976595) | Referencia oficial de ocupación y paro en perfiles cualificados. |
| `ISTAC - Empleo registrado según actividad económica CNAE25.csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Puestos cubiertos en sectores técnicos e ingeniería a nivel regional. |
| `ISTAC - Empleo registrado según divisiones...csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Series históricas de empleo desglosadas por divisiones de actividad. |
| `ISTAC - Empleo registrado según situaciones...csv` | ISTAC (Canarias) | [Portal de Datos Abiertos ISTAC](https://www.gobiernodecanarias.org/istac/) | Evaluación de la situación laboral (asalariados, autónomos) en Canarias. |
| `5mb25. Afiliados extranjeros... CNAE 25...csv` | Seguridad Social (TGSS) | [BBDD Estadísticas TGSS](https://w6.seg-social.es/PXWeb/pxweb/es/) | Aislar específicamente el sector TIC (CNAE Sección J) para talento tecnológico. |

---

## 🚀 Despliegue Rápido
Para levantar la infraestructura base de bases de datos persistentes:
```bash
docker compose up -d
```