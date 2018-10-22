#!/usr/bin/env bash

echo "airflow initdb"
airflow initdb

echo "airflow webserver -p 8080"
airflow webserver -p 8080

echo "airflow scheduler"
airflow scheduler
