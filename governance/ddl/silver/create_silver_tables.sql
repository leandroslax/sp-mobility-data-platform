-- Silver layer tables

CREATE TABLE IF NOT EXISTS sp_mobility_silver.gtfs
USING DELTA
LOCATION 'abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs';

CREATE TABLE IF NOT EXISTS sp_mobility_silver.sptrans_vehicle_positions
USING DELTA
LOCATION 'abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions';
