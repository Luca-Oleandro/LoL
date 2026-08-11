# Dockerfile — custom Airflow image with the LoL project dependencies already installed
# Base image: official Airflow, matching the version used across the project
FROM apache/airflow:2.10.3-python3.11

# Copy requirements.txt to save it in the cache and avoid reinstall at every change of code
COPY requirements.txt /requirements.txt

# Install as the "airflow" user (not root) - Airflow runs as this user at runtime, so packages installed as root wouldn't be visible to it
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt