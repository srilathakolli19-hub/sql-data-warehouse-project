use datawarehouse;
go
if OBJECT_ID ('silver.crm_cust_info','u') is not null
   drop table silver.crm_cust_info;
create table silver.crm_cust_info (
      cst_id INT,
      cst_key NVARCHAR(50),
      cst_firstname NVARCHAR(50),
      cst_lastname NVARCHAR(50),
      cst_marital_status NVARCHAR(50),
      cst_gndr NVARCHAR(50),
      cst_create_date DATE,
      dwh_create_date datetime2 default getdate()
);
if OBJECT_ID ('silver.crm_prd_info','u') is not null
   drop table silver.crm_prd_info;
create table silver.crm_prd_info (
      prd_id INT,
      cat_id nvarchar(50),
      prd_key NVARCHAR(50),
      prd_nm VARCHAR(50),
      prd_cost INT,
      prd_line NVARCHAR(50),
      prd_start_dt date,
      prd_end_dt date,
      dwh_create_date datetime2 default getdate()
);
if OBJECT_ID ('silver.crm_sales_details','u') is not null
   drop table silver.crm_sales_details;
create table silver.crm_sales_details (
      sls_ord_num NVARCHAR(50),
      sls_prd_key NVARCHAR(50),
      sls_cust_id INT,
      sls_order_dt date,
      sls_ship_dt date,
      sls_due_dt date,
      sls_sales INT,
      sls_quantity INT,
      sls_price INT,
      dwh_create_date datetime2 default getdate()

);
if OBJECT_ID ('silver.erp_loc_a101','u') is not null
   drop table silver.erp_loc_a101;
create table silver.erp_loc_a101 (
      cid NVARCHAR(50),
      cntry NVARCHAR(50),
      dwh_create_date datetime2 default getdate()
);
if OBJECT_ID ('silver.erp_cust_az12','u') is not null
   drop table silver.erp_cust_az12;
create table silver.erp_cust_az12 (
      cid NVARCHAR(50),
      bdate DATE,
      gen NVARCHAR(50),
      dwh_create_date datetime2 default getdate()
);
if OBJECT_ID ('silver.erp_px_cat_g1v2','u') is not null
   drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
      id nvarchar(50),
      cat NVARCHAR(50),
      subcat NVARCHAR(50),
      maintenance NVARCHAR(50),
      dwh_create_date datetime2 default getdate()
);
