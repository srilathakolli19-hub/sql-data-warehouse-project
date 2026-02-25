/*
======================================================================
Quality checks
======================================================================
Script purpose:
   This script performs various quality checks for data consistency,accuracy
   and standardization across the 'silver' schemas.it includes checks for:
   -NULL or duplicate primary keys.
   -UNwanted spaces in string fields
   -Data standardization and consistency
   -Invalid date ranges and orders.
   -Data consistency between related fields.
Usage notes:
   -Run these checks after loading silver layer.
   -Investigate and resolve any descrancies found during the checks.
======================================================================
*/


-- testing silver.crm_cust_info

-- checking for duplicates in primary key
-- expectation: no result
select 
cst_id,
COUNT(*)
from silver.crm_cust_info
group by cst_id
having COUNT(*)>1 or cst_id is null;

-- check for unwanted spaces
-- expectation: no results
select cst_firstname
from silver.crm_cust_info 
where cst_firstname!=TRIM(cst_firstname);

-- data standardization and consistency(for data cardinality)
select distinct cst_gndr from silver.crm_cust_info;

select * from silver.crm_cust_info;


-- testing silver.crm_prd_info

-- checking duplicates in product table 
-- expectation: no results

select prd_id,COUNT(*) from silver.crm_prd_info group by prd_id having COUNT(*)>1 or prd_id is null

-- check for unwanted spaces
-- expectation: no results
select prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for nulls or any negative costs
select prd_cost 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- data standardization and consistency
select distinct prd_line 
from silver.crm_prd_info

-- check for invalid date orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * from silver.crm_prd_info

--testing silver.crm_sales_details
-- checking for invalid date in sales details
select
sls_order_dt
from silver.crm_sales_details
where sls_order_dt <=0
or len(sls_order_dt)!=8
OR sls_order_dt < '1900-01-01'
OR sls_order_dt > '2050-01-01';

-- check for invalid date orders 
select 
*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--check data consistency : between sales,quantity, and price
-- >> sales=quantity*price
-- >> values must not ber null,zero,or negative
select
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<=0 or sls_quantity <=0 or sls_price <=0




--transforming check


select
case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price)
      then sls_quantity*ABS(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price<=0
     then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details



select * from silver.crm_sales_details


--testing silver.erp_cust_az12

--identify out of range dates in erp_cust_az12
select distinct
bdate
from silver.erp_cust_az12
where bdate > getdate()


-- check data standardization for gender
select distinct
gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12


-- testing silver.erp_loc_a101
-- data standardization
select distinct 
cntry
from silver.erp_loc_a101

--testing silver.erp_px_cat_g1v2
-- the data quality of this table is good
select * from silver.erp_px_cat_g1v2

