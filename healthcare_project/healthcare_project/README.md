# Real-Time Healthcare Data Processing

A simple base project that simulates patient monitors streaming vitals, processes
them in real time, raises alerts, and stores everything in SQLite.

## Pipeline
Simulated monitors -> Queue (stream) -> Processor -> SQLite

The processor performs:
- Sensor data validation (rejects impossible values)
- Threshold-based alerts (WARNING / CRITICAL)
- Statistical anomaly detection per patient (z-score, NOTICE)
- Storage of vitals and alerts in `healthcare.db`

## Requirements
Python 3.8+ (no extra packages)

## Run
    python healthcare_stream.py                 # 20 seconds, 5 patients
    python healthcare_stream.py -d 60 -p 10     # 60 seconds, 10 patients

Options: -d duration (s), -p patients, -i interval (s), --db database path

## Query results
    sqlite3 healthcare.db "SELECT * FROM alerts LIMIT 10;"

Note: Thresholds are simplified for demonstration and are not medical advice.
