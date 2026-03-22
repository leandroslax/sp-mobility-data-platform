-- Bronze layer tables

CREATE TABLE IF NOT EXISTS sp_mobility_bronze.gtfs_static
USING DELTA
LOCATION 'abfss://bronze@stspmobilitydev001dev001.dfs.core.windows.net/gtfs';

CREATE TABLE IF NOT EXISTS sp_mobility_bronze.sptrans_vehicle_positions
USING DELTA
LOCATION 'abfss://bronze@stspmobilitydev001dev001.dfs.core.windows.net/sptrans/vehicle_positions';
