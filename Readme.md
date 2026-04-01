# 🏏 IPL Data Engineering Pipeline (PySpark + Airflow + Docker)

This project is an **end-to-end Data Engineering pipeline** built using **PySpark, Airflow, and Docker**. It processes raw IPL ball-by-ball data and transforms it into structured datasets using the **Medallion Architecture (Bronze → Silver → Gold).**

> ⚠️ This project is created for **learning and practice purposes only**.

---

## 📊 Architecture Overview

The pipeline follows a layered **Medallion Architecture**:

### 🥉 Bronze Layer (Raw Ingestion)
- Ingests raw IPL CSV data  
- Minimal transformations  
- Stores data in **Parquet format**

---

### 🥈 Silver Layer (Data Cleaning & Transformation)
- Cleans and filters invalid records  
- Creates derived columns:
  - `is_boundary`
  - `is_wicket`  
- Keeps only valid deliveries  
- Structures data for analytics  

---

### 🥇 Gold Layer (Analytics & Aggregations)
- Generates insights:
  - Batter performance  
  - Bowler statistics  
  - Match summaries  
  - Venue analysis  
- Produces analytics-ready datasets  

---

## ⚙️ Tech Stack

- Apache Spark 3.5  
- PySpark  
- Apache Airflow  
- Docker & Docker Compose  
- Python  
- Parquet  


---

## 📦 Dataset

Source: Kaggle  
https://www.kaggle.com/datasets/chaitu20/ipl-dataset2008-2025  

---
## 🚀 How to Run the Project

### 1️⃣ Start services

```bash
docker compose up -d
```
docker compose up -d

### 2️⃣ Open Airflow UI

http://localhost:8084

Login using credentials from .env

### 3️⃣ Run Pipeline
Enable DAG: ipl_pipeline
Trigger manually

#### Pipeline flow:
Bronze → Silver → Gold

🔍 Spark UI

http://localhost:8082

---

⚠️ Note

This project is for learning purposes only and not production-ready

---