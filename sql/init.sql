-- Tres etapas de datos, en esquemas separados
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS train;

-- ETAPA 1: datos crudos, tal como los entrega la API (todo TEXT)
CREATE TABLE IF NOT EXISTS raw.covertype_raw (
    id                                  SERIAL PRIMARY KEY,
    elevation                           TEXT,
    aspect                              TEXT,
    slope                               TEXT,
    horizontal_distance_to_hydrology    TEXT,
    vertical_distance_to_hydrology      TEXT,
    horizontal_distance_to_roadways     TEXT,
    hillshade_9am                       TEXT,
    hillshade_noon                      TEXT,
    hillshade_3pm                       TEXT,
    horizontal_distance_to_fire_points  TEXT,
    wilderness_area                     TEXT,
    soil_type                           TEXT,
    cover_type                          TEXT,
    batch_number                        INTEGER,
    dag_run_id                          TEXT,
    ingested_at                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_batch ON raw.covertype_raw (batch_number);

-- ETAPA 2: datos procesados, con tipos correctos y sin nulos
CREATE TABLE IF NOT EXISTS clean.covertype_clean (
    id                                  SERIAL PRIMARY KEY,
    elevation                           INTEGER,
    aspect                              INTEGER,
    slope                               INTEGER,
    horizontal_distance_to_hydrology    INTEGER,
    vertical_distance_to_hydrology      INTEGER,
    horizontal_distance_to_roadways     INTEGER,
    hillshade_9am                       INTEGER,
    hillshade_noon                      INTEGER,
    hillshade_3pm                       INTEGER,
    horizontal_distance_to_fire_points  INTEGER,
    wilderness_area                     VARCHAR(50),
    soil_type                           VARCHAR(50),
    cover_type                          INTEGER,
    processed_at                        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETAPA 3: dataset listo para entrenamiento, con particion train/test
CREATE TABLE IF NOT EXISTS train.covertype_train (
    id                                  SERIAL PRIMARY KEY,
    elevation                           INTEGER,
    aspect                              INTEGER,
    slope                               INTEGER,
    horizontal_distance_to_hydrology    INTEGER,
    vertical_distance_to_hydrology      INTEGER,
    horizontal_distance_to_roadways     INTEGER,
    hillshade_9am                       INTEGER,
    hillshade_noon                      INTEGER,
    hillshade_3pm                       INTEGER,
    horizontal_distance_to_fire_points  INTEGER,
    wilderness_area                     VARCHAR(50),
    soil_type                           VARCHAR(50),
    cover_type                          INTEGER,
    split                               VARCHAR(10),
    prepared_at                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_train_split ON train.covertype_train (split);
