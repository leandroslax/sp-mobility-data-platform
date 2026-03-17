# SP Mobility Data Platform

Data engineering platform for São Paulo urban mobility analytics built with Azure, Databricks, Delta Lake and Terraform.

## Overview

This project builds an end-to-end lakehouse data platform to ingest, process and analyze São Paulo urban mobility data using public datasets and near real-time APIs.

The platform follows modern data engineering practices including:

- Infrastructure as Code (Terraform)
- Lakehouse architecture (Delta Lake)
- Medallion data model (Bronze, Silver, Gold)
- Data quality and governance
- Workflow orchestration
- Analytics dashboard

## Tech Stack

- Azure Databricks
- Azure Data Lake Storage Gen2
- Delta Lake
- Azure Event Hubs
- Azure Key Vault
- Terraform
- Databricks Workflows
- Databricks SQL

## Architecture

Public APIs / Open Data → Ingestion → Bronze → Silver → Gold → Dashboard

## Project Structure

- terraform → infrastructure as code
- src → ingestion and transformation pipelines
- workflows → orchestration
- dashboards → analytics layer
- tests → quality and validation
- docs → architecture and design decisions

## Status

Project initialization in progress.
