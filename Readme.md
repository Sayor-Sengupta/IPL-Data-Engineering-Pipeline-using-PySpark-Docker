# 🏏 IPL Data Engineering Pipeline using PySpark & Docker

This project is a beginner-friendly end-to-end Data Engineering pipeline built using **PySpark** and **Docker**.  
The pipeline processes raw IPL ball-by-ball data and transforms it into structured layers following the **Medallion Architecture (Bronze → Silver → Gold).**

This is just a spark practice project.

The main goal of this project is to understand:

- Spark DataFrame transformations
- Distributed data processing
- Data lake layer architecture
- Running Spark inside Docker containers
- Handling real-world issues like file paths and permissions

---

## 📊 Architecture

### 🥉 Bronze Layer
- Reads raw IPL CSV data
- Performs minimal transformations
- Stores data in **Parquet format**

### 🥈 Silver Layer
- Cleans and filters data
- Creates derived columns like:
  - `is_boundary`
  - `is_wicket`
- Keeps only valid deliveries
- Selects important analytical columns

### 🥇 Gold Layer
- Aggregates cleaned data
- Generates match or player level insights
- Produces analytics-ready datasets

---

## ⚙️ Tech Stack

- **Apache Spark 3.5**
- **PySpark**
- **Docker**
- **Python**
- **Parquet (Data Lake Storage Format)**

---

## Dataset used
https://www.kaggle.com/datasets/chaitu20/ipl-dataset2008-2025

---


---

## 🚀 How to Run the Project

### Start Spark Cluster using Docker


docker compose up -d


###  Run Bronze Job


docker exec -it spark-master spark-submit /opt/spark-app/Ipl-pipeline/jobs/bronze_jobs.py


###  Run Silver Job


docker exec -it spark-master spark-submit /opt/spark-app/Ipl-pipeline/jobs/silver_jobs.py


###  Run Gold Job


docker exec -it spark-master spark-submit /opt/spark-app/Ipl-pipeline/jobs/gold_jobs.py


---

## Learning Outcomes

Through this project, I learned:

- How Spark reads and writes distributed data
- Importance of Parquet format in analytics
- Medallion data architecture concept
- Handling Docker volume permissions
- Debugging Spark job failures
- Understanding Spark execution logs and UI
---