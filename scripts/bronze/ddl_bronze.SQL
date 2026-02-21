/*
===================================================================================
DDL SCRIPT : CREATE BRONZE TABLES
===================================================================================
Script purpose:
    This script creates tables in the 'Bronze' schema,dropping existing tables
    if they already exist.
    run this script to redefine the DDL structure of 'bronze' tables.
===================================================================================
*/
use datawarehouse;
go
if OBJECT_ID ('bronze.crm_cust_info','u') is not null
   drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
      cst_id INT,
      cst_key NVARCHAR(50),
      cst_firstname NVARCHAR(50),
      cst_lastname NVARCHAR(50),
      cst_material_status NVARCHAR(50),
      cst_gndr NVARCHAR(50),
      cst_create_date DATE
);
if OBJECT_ID ('bronze.crm_prd_info','u') is not null
   drop table bronze.crm_prd_info;
create table bronze.crm_prd_info (
      prd_id INT,
      prd_key NVARCHAR(50),
      prd_nm VARCHAR(50),
      prd_cost INT,
      prd_line NVARCHAR(50),
      prd_start_dt DATETIME,
      prd_end_dt DATETIME
);
if OBJECT_ID ('bronze.crm_sales_details','u') is not null
   drop table bronze.crm_sales_details;
create table bronze.crm_sales_details (
      sls_ord_num NVARCHAR(50),
      sls_prd_key NVARCHAR(50),
      sls_cust_id INT,
      sls_order_dt INT,
      sls_ship_dt INT,
      sls_due_dt INT,
      sls_sales INT,
      sls_quantity INT,
      sls_price INT
);
if OBJECT_ID ('bronze.erp_loc_a101','u') is not null
   drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
      cid NVARCHAR(50),
      cntry NVARCHAR(50),
);
if OBJECT_ID ('bronze.erp_cust_az12','u') is not null
   drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
      cid NVARCHAR(50),
      bdate DATE,
      gen NVARCHAR(50)
);
if OBJECT_ID ('bronze.erp_px_cat_g1v2','u') is not null
   drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
      id nvarchar(50),
      cat NVARCHAR(50),
      subcat NVARCHAR(50),
      maintenance NVARCHAR(50)
);
