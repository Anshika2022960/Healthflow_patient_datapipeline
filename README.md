# HealthFlow – Patient Vital Monitoring Pipeline

##  Project Overview
HealthFlow is a real-time healthcare data pipeline that streams patient vital signs, validates and processes them, and stores the data in HDFS for further analysis.
This project simulates how modern healthcare systems handle continuous patient monitoring data using big data technologies.

## Architecture
Patient Data Generator  
→ Kafka (Confluent Cloud)  
→ Python Consumer (Validation & Transformation)  
→ HDFS (Cloudera VM)

## Technologies Used
- Python
- Apache Kafka (Confluent Cloud)
- Apache Hadoop HDFS
- WebHDFS
- Cloudera QuickStart VM
- CSV File Format

## Data Fields
- patient_id
- timestamp
- heart_rate
- temperature
- systolic_bp
- diastolic_bp
- processed_at
- ingest_time
- is_high_bp

## How It Works
1. Patient vitals are generated and sent to Kafka.
2. Python consumer reads Kafka messages.
3. Data is validated (range checks).
4. Valid records are transformed.
5. Data is stored in HDFS as CSV files.
6. Files are partitioned by date.

## HDFS Storage Structure
/healthflow/patient_data/
└── date=YYYY-MM-DD/
└── patient_data.csv

## How to Run
1. Start Kafka producer.
2. Run Python producer and consumer.
3. Verify files in HDFS:
hdfs dfs -ls /healthflow/patient_data

## Use Case
- Remote patient monitoring
- Healthcare analytics
- Real-time alert systems
- Big data streaming practice

---
Monalisa Mohapatra

Euron Internship Project – 2025

