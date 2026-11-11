-- Creación de esquemas para la capa Raw y auditoría de carga
CREATE SCHEMA IF NOT EXISTS src;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA src IS 'Esquema de origen para albergar datos crudos de fuentes CSV sin transformar';
COMMENT ON SCHEMA audit IS 'Esquema para trazabilidad y logs de ejecución del proceso de ingesta';