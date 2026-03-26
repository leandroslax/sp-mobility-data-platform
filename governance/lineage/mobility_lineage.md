# Data Lineage - SP Mobility Platform

## Visão geral
A plataforma segue arquitetura Medallion com camadas Raw, Bronze, Silver e Gold.

## Fluxo principal

### GTFS
GTFS files
→ raw/gtfs
→ bronze.gtfs_routes / gtfs_trips / gtfs_stops / gtfs_stop_times / gtfs_calendar
→ silver datasets tratados
→ gold métricas analíticas

### SPTrans
SPTrans API vehicle positions
→ raw/sptrans
→ bronze.sptrans_vehicle_positions
→ silver.sptrans_vehicle_positions
→ gold.mobility_analytics
→ gold.mobility_intelligence
→ gold.route_performance
→ gold.city_activity
→ gold.city_heatmap
→ gold.mobility_kpis

## Objetivo
Garantir rastreabilidade desde a origem até os datasets analíticos finais.
