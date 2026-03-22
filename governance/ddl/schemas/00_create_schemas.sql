-- Create schemas for Medallion architecture using ADLS locations

CREATE DATABASE IF NOT EXISTS sp_mobility_bronze
LOCATION 'abfss://bronze@stspmobilitydev001dev001.dfs.core.windows.net/_metastore/sp_mobility_bronze';

CREATE DATABASE IF NOT EXISTS sp_mobility_silver
LOCATION 'abfss://silver@stspmobilitydev001dev001.dfs.core.windows.net/_metastore/sp_mobility_silver';

CREATE DATABASE IF NOT EXISTS sp_mobility_gold
LOCATION 'abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/_metastore/sp_mobility_gold';
