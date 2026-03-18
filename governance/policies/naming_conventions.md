# Naming Conventions

## Objetivo
Padronizar nomes de datasets, tabelas e colunas na plataforma de mobilidade.

## Regras para tabelas

Formato:

layer_dataset_entity

Exemplos:

bronze_gtfs_routes
bronze_gtfs_trips
silver_vehicle_positions
gold_mobility_kpis

## Regras para colunas

- snake_case
- nomes descritivos
- evitar abreviações ambíguas

Exemplos:

event_date
event_hour
vehicle_id
route_id
latitude
longitude

## Regras para camadas

bronze → dados brutos estruturados  
silver → dados tratados e enriquecidos  
gold → dados analíticos e agregados
