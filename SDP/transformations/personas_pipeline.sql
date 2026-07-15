-- ============================================================================
-- personas_pipeline.sql
-- Pipeline declarativo "personas_sdp"
--
-- Este archivo vive en la carpeta transformations/ de un ETL Pipeline.
--
-- Requisitos del pipeline (ver README.md):
--   * Catálogo por defecto: big_data_ii_2025   ·   Schema por defecto: spark_examples
--   * Parámetro de configuración:  source = /Volumes/big_data_ii_2025/spark_examples/sdp_landing
--   * Serverless, triggered.
--
-- ============================================================================


-- ----------------------------------------------------------------------------
-- CAPA BRONZE — streaming table de ingesta cruda
-- ${source} se reemplaza en runtime por el valor configurado en el pipeline.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE personas_bronze (
  CONSTRAINT id_presente EXPECT (id IS NOT NULL)
  -- sin ON VIOLATION => acción "warn": la fila SE ESCRIBE
  -- y la violación queda contabilizada en las métricas
)
COMMENT 'Bronze: ingesta incremental de archivos CSV de personas'
AS SELECT
     *,
     _metadata.file_path AS _source_file,
     current_timestamp() AS _ingest_ts
   FROM STREAM read_files(
     '${source}/personas',
     format => 'csv',
     header => true
   );


-- ----------------------------------------------------------------------------
-- Vista temporal para hacer una transformacion intermedia.
-- NO se registra en Unity Catalog.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW vw_personas_std AS
SELECT
  CAST(id AS BIGINT)   AS id,
  TRIM(nombre)         AS nombre,
  CAST(edad AS INT)    AS edad,
  TRIM(ciudad)         AS ciudad,
  _source_file,
  _ingest_ts
FROM STREAM personas_bronze;


-- ----------------------------------------------------------------------------
-- CAPA SILVER — streaming table con reglas de calidad
-- personas_001.csv trae, a propósito:
--   4 edades negativas  -> las descarta edad_valida  (DROP)
--   3 nombres vacíos    -> las descarta nombre_lleno (DROP)
--   2 ciudades vacías   -> las contabiliza ciudad_presente (warn)
-- 40 filas de entrada  =>  33 escritas + 7 descartadas
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE personas_silver (
  CONSTRAINT edad_valida     EXPECT (edad >= 0)
    ON VIOLATION DROP ROW,
  CONSTRAINT nombre_lleno    EXPECT (nombre IS NOT NULL AND nombre != '')
    ON VIOLATION DROP ROW,
  CONSTRAINT ciudad_presente EXPECT (ciudad IS NOT NULL AND ciudad != '')
  -- warn: visibilidad sin pérdida de datos
)
COMMENT 'Silver: personas estandarizadas y validadas'
AS SELECT * FROM STREAM vw_personas_std;

-- Tercera acción (NO activar con datos sucios presentes: detiene el flow):
--   CONSTRAINT id_critico EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE


-- ----------------------------------------------------------------------------
-- CAPA GOLD — materialized view de consumo
-- Nota: la fuente se referencia SIN la palabra STREAM; la MV rastrea
-- los cambios y se refresca en cada update (incremental cuando es posible).
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW personas_por_ciudad
COMMENT 'Gold: conteo y edad promedio por ciudad, listo para BI'
AS SELECT
     ciudad,
     COUNT(*)                    AS personas,
     ROUND(AVG(edad), 1)         AS edad_promedio
   FROM personas_silver
   GROUP BY ciudad;
