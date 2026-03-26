-- Gold layer tables

CREATE TABLE IF NOT EXISTS sp_mobility_gold.route_performance
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/route_performance';

CREATE TABLE IF NOT EXISTS sp_mobility_gold.sptrans_analytics
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/sptrans_analytics';

CREATE TABLE IF NOT EXISTS sp_mobility_gold.mobility_intelligence
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility/intelligence';

CREATE TABLE IF NOT EXISTS sp_mobility_gold.mobility_kpis
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility_kpis';

CREATE TABLE IF NOT EXISTS sp_mobility_gold.city_heatmap
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/map/city_heatmap';

CREATE TABLE IF NOT EXISTS sp_mobility_gold.city_activity
USING DELTA
LOCATION 'abfss://gold@stspmobilitydev001.dfs.core.windows.net/city_activity';
