# Redin Analytics ETL Pipeline

Welcome to the **Redin Analytics ETL Pipeline**! This project demonstrates an end-to-end data engineering workflow, orchestrating the collection, transformation, storage, and analysis of real estate market data. The pipeline uses a combination of industry-standard tools—**Airflow, Python, Pandas, Snowflake, AWS S3, and Power BI**—to deliver a scalable and automated data solution.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture Diagram](#architecture-diagram)
- [Tools & Technologies](#tools--technologies)
- [Repo Structure](#repo-structure)
- [Pipeline Workflow](#pipeline-workflow)
- [Airlfow DAG Diagram](#airflow-dag-diagram)
- [Why Use Pandas Chunk Processing?](#why-use-pandas-chunk-processing)
- [Possible Improvements](#possible-improvements)


---

## Project Overview

This project automates the process of extracting real estate market data from an external website(Redfin), transforming it for analysis, and loading it into a cloud data warehouse for dashboarding and further insights. The process is scheduled to run **weekly** via Airflow, ensuring that the latest data is always available for business intelligence and analytics.

---

## Architecture Diagram

<img width="4632" height="2460" alt="Image" src="https://github.com/user-attachments/assets/78dfc688-f0f6-4ed5-9262-1afa50615d5e" />

---

## Tools & Technologies

- **Apache Airflow**: Orchestrates and automates the pipeline scheduling and task dependencies.
- **Python & Pandas**: Handles the extraction, transformation, and local processing of raw data.
- **AWS S3**: Serves as a cloud-based storage solution for processed data in Parquet format.
- **Snowflake**: Acts as the data warehouse, enabling scalable storage and querying.
- **Power BI**: Visualizes the data, helping stakeholders make data-driven decisions.

---

## Repo Structure
```
📂 Redfin-analytics-etl-pipeline
│── 📂 dags             # Python DAG file
│── 📂 dashboard        # PowerBI screenshots
│── 📂 sql_worksheets   # Snowflake SQL worksheets
│── 📄 README.md        # Project Documentation
```


## Pipeline Workflow

1. **Data Extraction**
   - The pipeline begins by downloading raw data from the following public URL:
     ```
     https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz
     ```
   - The data is downloaded as a `.tsv.gz` file and saved locally.

2. **Data Transformation**
   - The raw data is processed using **Pandas**. All transformation steps (such as cleaning, formatting, and feature engineering) are performed in the code. *(Refer to the code in this repo for specific transformation logic.)*

3. **S3 File Management**
   - Before uploading new data, all existing files in the designated S3 bucket are deleted to prevent redundancy. This ensures that only the latest, complete dataset is stored.

4. **Data Upload to AWS S3**
   - The cleaned data is saved as Parquet files and uploaded to an **AWS S3 bucket**.

5. **Local Cleanup**
   - All intermediate and temporary files created during the process are deleted from the local system to save space.

6. **Orchestration & Scheduling**
   - Steps 1–5 are orchestrated and scheduled via **Airflow** to run automatically every week.

7. **Data Warehousing with Snowflake**
   - Processed data files in S3 are loaded into **Snowflake** tables using **Snowpipe**. *(For details, refer to the Snowflake worksheet in the repo.)*

8. **Business Intelligence with Power BI**
   - The Snowflake data warehouse is connected to **Power BI** for dashboards and visualizations. *(You can find the Power BI dashboard in the repo for reference.)*

---
## Airflow DAG Diagram

<img width="778" height="632" alt="Image" src="https://github.com/user-attachments/assets/a705c830-05d9-4743-aa62-6ee6dbdebf54" />
---

## Why Use Pandas Chunk Processing?

When dealing with large datasets (sometimes several gigabytes in size), loading all the data into memory at once can overwhelm local system resources and cause crashes. To address this, the pipeline uses **Pandas chunk processing**, reading and processing the data in smaller, manageable chunks. This approach allows for efficient memory usage and ensures that even large datasets can be processed on machines with limited RAM.

---

## Possible Improvements

While the current pipeline is robust and effective, there are areas where it could be further optimized:

- **In-Database Transformations**: Instead of performing all transformations locally with Pandas, we could offload transformation logic to **Snowflake** using SQL. This would leverage the scalable compute resources of the cloud warehouse, simplify local processing, and improve maintainability.
- **Incremental Loading**: Instead of deleting and re-uploading all data each time, consider implementing incremental data loads to optimize cloud storage and reduce compute costs.
- **Enhanced Monitoring**: Integrate better monitoring and alerting within Airflow or with third-party tools for improved visibility into pipeline health.

---
*Feel free to reach out or open an issue if you have questions or suggestions!*
