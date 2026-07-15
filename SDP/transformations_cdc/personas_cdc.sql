-- ============================================================================
-- personas_cdc.sql
--
-- Para activarla: en el editor del pipeline, agrega esta carpeta
-- (transformations_opcional/) como source code path adicional
-- (Settings -> Code assets -> Configure paths), o copia este archivo
-- dentro de transformations/. Luego Run pipeline.
--
-- Fuente de cambios: ${source}/personas_updates  (personas_updates_001.csv)
-- Columnas: id, nombre, edad, ciudad, operacion, fecha_proceso
-- ============================================================================


-- 1) Streaming table raw con los eventos de cambio (inserts/updates/deletes)
CREATE OR REFRESH STREAMING TABLE personas_updates_raw
COMMENT 'Eventos CDC con timestamp'
AS SELECT *
   FROM STREAM read_files(
     '${source}/personas_updates',
     format => 'csv',
     header => true
   );


-- 2) Tabla destino que mantendrá el estado actual (SCD Tipo 1)
CREATE OR REFRESH STREAMING TABLE personas_scd1
COMMENT 'Estado actual de personas aplicando CDC (sin historia)';


-- 3) Flow declarativo de CDC (antes: APPLY CHANGES INTO)
CREATE FLOW personas_cdc_flow AS
AUTO CDC INTO personas_scd1
  FROM STREAM personas_updates_raw
  KEYS (id)                                    -- llave para casar filas
  APPLY AS DELETE WHEN operacion = 'DELETE'    -- cuándo tratar como borrado
  SEQUENCE BY fecha_proceso                    -- orden lógico de los eventos
  COLUMNS * EXCEPT (operacion)                 -- columnas que llegan al destino
  STORED AS SCD TYPE 1;                        -- sobrescribir (sin historia)

-- Resultado esperado con personas_updates_001.csv:
--   id 9001 -> queda con su ÚLTIMA versión (edad 35, San José), por SEQUENCE BY
--   id 9002 -> eliminado (operacion = DELETE)
--   id 9003 -> insertado
--   personas_scd1 termina con 2 filas.
--
-- Reto: cambia a STORED AS SCD TYPE 2 y observa las columnas
-- __START_AT / __END_AT que versionan la historia.
