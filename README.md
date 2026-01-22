# ETL Pipeline: Local CSV → PostgreSQL using Airflow

## Project Overview
This project demonstrates a simple ETL (Extract-Transform-Load) pipeline using **Apache Airflow**, **PostgreSQL**, and **Python**. The pipeline extracts data from a local CSV file and loads it into a PostgreSQL database running in Docker.  

The DAG is designed to be modular, reusable, and easy to extend for other datasets.

---

## Features
- Extracts data from a local CSV file
- Loads data into a PostgreSQL table
- Uses Docker Compose to manage services:
  - Airflow (scheduler + webserver)
  - PostgreSQL
  - pgAdmin for database visualization
- Logs ETL runs in Airflow UI
- Table created with proper column types
- DAG runs in **append mode** to prevent overwriting data
- Easy to extend for additional transformations

---

## Project Structure

