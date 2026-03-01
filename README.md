# Data Warehouse and Analytics Project

Welcome to the Data Warehouse and Analytics Project repository! 🚀  
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---

📌 Project Overview

This project involves:

🔹 Data Architecture:
Designing a modern data warehouse using Medallion Architecture with Bronze, Silver, and Gold layers.

🔹 ETL Pipelines:
Extracting, transforming, and loading data from source systems into the data warehouse.

🔹 Data Modeling:
Developing fact and dimension tables optimized for analytical queries.

🔹 Analytics and Reporting:
Creating SQL-based reports and dashboards to deliver actionable insights.



🚀 Who This Repository Is For

This repository is an excellent resource for professionals and students looking to showcase expertise in:
1)SQL Development
2)Data Architecture
3)Data Engineering
4)ETL Pipeline Development
5)Data Modeling
6)Data Analytics

## 🚀 Project Requirements


### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- *Data Sources:* Import data from two source systems (ERP and CRM) provided as CSV files.
- *Data Quality:* Cleanse and resolve data quality issues prior to analysis.
- *Integration:* Combine both sources into a single, user-friendly data model designed for analytical queries.
- *Scope:* Focus on the latest dataset only; historization of data is not required.
- *Documentation:* Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analytics)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:

- Customer Behavior  
- Product Performance  
- Sales Trends  

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

---
## Data warehouse Architecture
<h2 align="center">🏗️ Data Warehouse Architecture</h2>

<p align="center">
  <img src="docs/DATAWAREHOUSE_ARCHITECTURE.png" width="900"/>
</p>




## 📂 Repository Structure

```text
├── README.md
├── LICENSE
├── init_database.sql

├── datasets/
│   ├── CRM/
│   │   ├── customer_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── ERP/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv

├── docs/
│   ├── Data_Warehouse_Architecture.png
│   ├── Data_Flow.png
│   ├── Data_Model.png
│   ├── Data_Catalog.txt
│   └── Star_Schema.png

├── scripts/
│   ├── Bronze/
│   │   ├── ddl_bronze.sql
│   │   └── proc_load_bronze.sql
│   ├── Silver/
│   │   ├── ddl_silver.sql
│   │   └── proc_load_silver.sql
│   └── Gold/
│       └── ddl_gold.sql

└── tests/
    ├── qualitychecks_silver.sql
    └── qualitychecks_gold.sql

## 📄 License

This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

---

## ⭐ About Me

Hi there! I'm Srilatha Kolli, and I'm passionate about data 📊, pipelines, visualization, and generating meaningful insights.  
I enjoy working with data to solve real-world problems and turn raw information into valuable business decisions.
