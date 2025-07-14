# Demo DuckDB

El presente demo muestra como se utilizan los cubos de OLAP utilizando el motor de OLAP Open Source duckdb.

**Objetivo**: crear un mini‑data‑warehouse local, ejecutar algunas transformaciones en el dataset de wine-quality y demostrar `GROUP BY CUBE`

# Instalación

Para instalar DuckDB es necesario seguir las instrucciones en [DuckDB Installation](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=win&download_method=direct&architecture=x86_64)

En el caso de OSX, es facil instalarlo con `brew install duckdb`

Una vez instalada se puede probar utilizando:

```
duckdb --version
```

# Preparación

Crear un directorio donde se va a guardar la base de datos. En el caso de OSX, son las siguientes instrucciones:

```bash
mkdir duckdb_demo
cd duckdb_demo
duckdb demo.duckdb
```

Deberia mostrar lo siguiente:

```bash
❯ duckdb demo.duckdb
DuckDB v1.3.2 (Ossivalis) 0b83e5d2f6
Enter ".help" for usage hints.
D
```

# Carga de datos

Para cargar el dataset de wine-quality, específicamente el de vinos rojos, lo cargamos directamente del repositorio de Machine Learning de UCI:

Desde la consola misma de duckdb, ejecutar las siguientes instrucciones:

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE TABLE staging AS
SELECT *
FROM read_csv_auto(
  'https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv',
  AUTO_DETECT = TRUE,
  HEADER      = TRUE
);
```

Comprobamos que fue cargado correctamente:

```sql
SELECT * FROM staging LIMIT 5;
```
Resultado:
```
┌───────────────┬──────────────────┬─────────────┬────────────────┬───────────┬───┬────────┬───────────┬─────────┬─────────┐
│ fixed acidity │ volatile acidity │ citric acid │ residual sugar │ chlorides │ … │   pH   │ sulphates │ alcohol │ quality │
│    double     │      double      │   double    │     double     │  double   │   │ double │  double   │ double  │  int64  │
├───────────────┼──────────────────┼─────────────┼────────────────┼───────────┼───┼────────┼───────────┼─────────┼─────────┤
│           7.4 │              0.7 │         0.0 │            1.9 │     0.076 │ … │   3.51 │      0.56 │     9.4 │       5 │
│           7.8 │             0.88 │         0.0 │            2.6 │     0.098 │ … │    3.2 │      0.68 │     9.8 │       5 │
│           7.8 │             0.76 │        0.04 │            2.3 │     0.092 │ … │   3.26 │      0.65 │     9.8 │       5 │
│          11.2 │             0.28 │        0.56 │            1.9 │     0.075 │ … │   3.16 │      0.58 │     9.8 │       6 │
│           7.4 │              0.7 │         0.0 │            1.9 │     0.076 │ … │   3.51 │      0.56 │     9.4 │       5 │
├───────────────┴──────────────────┴─────────────┴────────────────┴───────────┴───┴────────┴───────────┴─────────┴─────────┤
│ 5 rows                                                                                              12 columns (9 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

# Transformaciones

Ejecutamos las siguientes transformaciones:

```sql
-- Vino Rojo
ALTER TABLE staging ADD COLUMN wine_type VARCHAR;
UPDATE staging SET wine_type = 'red';

-- total_acidity = "fixed acidity" + "volatile acidity"
ALTER TABLE staging ADD COLUMN total_acidity DOUBLE;
UPDATE staging
SET total_acidity = "fixed acidity" + "volatile acidity";

-- totla acidity
ALTER TABLE staging ADD COLUMN acidity_level VARCHAR;
UPDATE staging
SET acidity_level = CASE
  WHEN total_acidity <  8 THEN 'Low'
  WHEN total_acidity < 12 THEN 'Medium'
  ELSE 'High'
END;
```

# Hechos y Dimensiones

Podemos separar la tabla en una tabla de hechos con la información que nos interesa, y una tabla de dimensiones con la información del nivel de acidez:

```sql
CREATE TABLE dim_acidity AS
SELECT DISTINCT acidity_level FROM staging;

CREATE TABLE fact_wine AS
SELECT
  ROW_NUMBER() OVER () AS id,
  quality,
  wine_type,
  acidity_level,
  total_acidity,
  alcohol
FROM staging;
```

Revisamos como quedan:

```sql
SELECT * FROM dim_acidity LIMIT 5;
```
Resultado:
```
┌───────────────┐
│ acidity_level │
│    varchar    │
├───────────────┤
│ Medium        │
│ Low           │
│ High          │
└───────────────┘
```

Y la tabla de hechos:

```sql
SELECT * FROM fact_wine LIMIT 5;
```
Resultado:
```
┌───────┬─────────┬───────────┬───────────────┬────────────────────┬─────────┐
│  id   │ quality │ wine_type │ acidity_level │   total_acidity    │ alcohol │
│ int64 │  int64  │  varchar  │    varchar    │       double       │ double  │
├───────┼─────────┼───────────┼───────────────┼────────────────────┼─────────┤
│     1 │       5 │ red       │ Medium        │                8.1 │     9.4 │
│     2 │       5 │ red       │ Medium        │               8.68 │     9.8 │
│     3 │       5 │ red       │ Medium        │               8.56 │     9.8 │
│     4 │       6 │ red       │ Medium        │ 11.479999999999999 │     9.8 │
│     5 │       5 │ red       │ Medium        │                8.1 │     9.4 │
└───────┴─────────┴───────────┴───────────────┴────────────────────┴─────────┘
```

Y podemos ver conteos por nivel de acidez:

```sql
SELECT acidity_level, COUNT(*) AS cnt
  FROM   fact_wine
  GROUP  BY acidity_level;
```
Resultado:
```
┌───────────────┬───────┐
│ acidity_level │  cnt  │
│    varchar    │ int64 │
├───────────────┼───────┤
│ Medium        │   932 │
│ Low           │   569 │
│ High          │    98 │
└───────────────┴───────┘
```

# GROUP BY CUBE

Las siguientes consultas muestran como una vez preparada la tabla podemos sacar ventaja y hacer consultas que aprovechen GROUP BY CUBE:

**Calidad y alcohol promedio por acidity_level**

```sql
SELECT
      quality,
      acidity_level,
      COUNT(*)              AS cnt_samples,
      ROUND(AVG(alcohol),2) AS avg_alcohol
  FROM   fact_wine
  GROUP  BY CUBE (quality, acidity_level)
  ORDER  BY quality, acidity_level;
```
Resultado:
```
┌─────────┬───────────────┬─────────────┬─────────────┐
│ quality │ acidity_level │ cnt_samples │ avg_alcohol │
│  int64  │    varchar    │    int64    │   double    │
├─────────┼───────────────┼─────────────┼─────────────┤
│       3 │ High          │           1 │         9.0 │
│       3 │ Low           │           3 │        9.98 │
│       3 │ Medium        │           6 │        10.1 │
│       3 │ NULL          │          10 │        9.96 │
│       4 │ High          │           3 │        9.97 │
│       4 │ Low           │          26 │       10.62 │
│       4 │ Medium        │          24 │        9.92 │
│       4 │ NULL          │          53 │       10.27 │
│       5 │ High          │          28 │       10.44 │
│       5 │ Low           │         236 │       10.08 │
│       5 │ Medium        │         417 │        9.76 │
│       5 │ NULL          │         681 │         9.9 │
│       6 │ High          │          43 │       10.58 │
│       6 │ Low           │         243 │       10.88 │
│       6 │ Medium        │         352 │       10.46 │
│       6 │ NULL          │         638 │       10.63 │
│       7 │ High          │          22 │       10.69 │
│       7 │ Low           │          55 │       11.76 │
│       7 │ Medium        │         122 │       11.47 │
│       7 │ NULL          │         199 │       11.47 │
│       8 │ High          │           1 │         9.8 │
│       8 │ Low           │           6 │       12.27 │
│       8 │ Medium        │          11 │       12.21 │
│       8 │ NULL          │          18 │       12.09 │
│    NULL │ High          │          98 │       10.52 │
│    NULL │ Low           │         569 │       10.63 │
│    NULL │ Medium        │         932 │       10.29 │
│    NULL │ NULL          │        1599 │       10.42 │
├─────────┴───────────────┴─────────────┴─────────────┤
│ 28 rows                                   4 columns │
└─────────────────────────────────────────────────────┘
```

Sobre los NULLS:

* `NULL` en **quality** muestra el subtotal por *acidity_level*  
* `NULL` en **acidity_level** muestra el subtotal por *quality*  
* `NULL / NULL`  → total general

**Porcentaje de vinos con acidez “High”**

```sql
WITH cube_stats AS (
  SELECT
      acidity_level,
      COUNT(*) AS cnt
  FROM fact_wine
  GROUP BY CUBE(acidity_level)
)
SELECT
    ROUND(100.0 * cnt /
        (SELECT cnt FROM cube_stats WHERE acidity_level IS NULL), 2) AS pct_high_acidity
FROM cube_stats
WHERE acidity_level = 'High';
```
Resultados:
```
┌──────────────────┐
│ pct_high_acidity │
│      double      │
├──────────────────┤
│       6.13       │
└──────────────────┘
```

Podemos ver la primera consulta que es lo que retorna:

```sql
  SELECT
      acidity_level,
      COUNT(*) AS cnt
  FROM fact_wine
  GROUP BY CUBE(acidity_level)
```
Resultado:
```
┌───────────────┬───────┐
│ acidity_level │  cnt  │
│    varchar    │ int64 │
├───────────────┼───────┤
│ NULL          │  1599 │
│ High          │    98 │
│ Medium        │   932 │
│ Low           │   569 │
└───────────────┴───────┘
```

**Comparar % de vinos por acidez y calidad**

```sql
WITH cube_stats AS (
  SELECT
      quality,
      acidity_level,
      COUNT(*) AS cnt
  FROM fact_wine
  GROUP BY CUBE(quality, acidity_level)
)
SELECT
    quality,
    acidity_level,
    cnt,
    ROUND(100.0 * cnt /
        (SELECT cnt FROM cube_stats WHERE quality IS NULL AND acidity_level IS NULL), 2)
        AS pct_total
FROM cube_stats
ORDER BY pct_total DESC;
```

Resultado:
```
┌─────────┬───────────────┬───────┬───────────┐
│ quality │ acidity_level │  cnt  │ pct_total │
│  int64  │    varchar    │ int64 │  double   │
├─────────┼───────────────┼───────┼───────────┤
│    NULL │ NULL          │  1599 │     100.0 │
│    NULL │ Medium        │   932 │     58.29 │
│       5 │ NULL          │   681 │     42.59 │
│       6 │ NULL          │   638 │      39.9 │
│    NULL │ Low           │   569 │     35.58 │
│       5 │ Medium        │   417 │     26.08 │
│       6 │ Medium        │   352 │     22.01 │
│       6 │ Low           │   243 │      15.2 │
│       5 │ Low           │   236 │     14.76 │
│       7 │ NULL          │   199 │     12.45 │
│       7 │ Medium        │   122 │      7.63 │
│    NULL │ High          │    98 │      6.13 │
│       7 │ Low           │    55 │      3.44 │
│       4 │ NULL          │    53 │      3.31 │
│       6 │ High          │    43 │      2.69 │
│       5 │ High          │    28 │      1.75 │
│       4 │ Low           │    26 │      1.63 │
│       4 │ Medium        │    24 │       1.5 │
│       7 │ High          │    22 │      1.38 │
│       8 │ NULL          │    18 │      1.13 │
│       8 │ Medium        │    11 │      0.69 │
│       3 │ NULL          │    10 │      0.63 │
│       8 │ Low           │     6 │      0.38 │
│       3 │ Medium        │     6 │      0.38 │
│       4 │ High          │     3 │      0.19 │
│       3 │ Low           │     3 │      0.19 │
│       8 │ High          │     1 │      0.06 │
│       3 │ High          │     1 │      0.06 │
├─────────┴───────────────┴───────┴───────────┤
│ 28 rows                           4 columns │
└─────────────────────────────────────────────┘
```

# Salir

Se sale de la aplicación y se guarda la base de datos utilizando el comando:

```
.quit
```
