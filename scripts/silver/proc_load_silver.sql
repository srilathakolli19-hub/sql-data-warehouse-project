/*
===========================================================
Stored procedure:load silver layer(bronze->silver)
===========================================================
Script purpose:
      this stored procedure performs etl(extract,transform,load) process to
      populate the 'silver' schema tables from the 'bronze' schema.
Actions performed:
      -truncates silver tables
      -inserts transformed and cleansed data from bronze into silver tables.
Parameters:
      None.
      This stored procedure does not accept any parameters or return any values.
Usage example:
      EXEC silver.load_silver;
===========================================================
*/



use DataWarehouse;
go

exec silver.load_silver;
create or alter procedure silver.load_silver as
begin
  declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
  begin try
        set @batch_start_time=getdate();
        
        print'==============================================='
        print'loading silver layer'
        print'==============================================='
        
        print'-----------------------------------------------'
        print'loading CRM tables'
        print'-----------------------------------------------'


        set @start_time=getdate();
        print 'Truncating table: silver.crm_cust_info';
        truncate table silver.crm_cust_info;
        print 'Inserting into : silver.crm_cust_info';
        insert into silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date)
        select
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname)as cst_lastname,
        case when UPPER(trim(cst_marital_status)) = 'S' then 'Single'
             when UPPER(trim(cst_marital_status)) = 'M' then 'Married'
             else 'n/a'
        end cst_marital_status,
        case when UPPER(trim(cst_gndr)) = 'F' then 'Female'
             when UPPER(trim(cst_marital_status)) = 'M' then 'Male'
             else 'n/a'
        end cst_gndr,
        cst_create_date
        from(
           select 
           *,
           row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
           from bronze.crm_cust_info
           where cst_id is not null
        )t where flag_last=1;
        set @end_time = getdate();
        print 'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>--------------';


        set @start_time=getdate();
        print 'Truncating table: silver.crm_prd_info';
        truncate table silver.crm_prd_info;
        print 'Inserting into : silver.crm_prd_info';
        insert into silver.crm_prd_info (
              prd_id,
              cat_id,
              prd_key,
              prd_nm,
              prd_cost,
              prd_line, 
              prd_start_dt, 
              prd_end_dt
        )
        select 
        prd_id,
        REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        coalesce(prd_cost,0) as prd_cost,
        case UPPER(trim(prd_line))
             when 'M' then 'Mountain'
             when 'R' then 'Road'
             when 'S' then 'Other Sales'
             when 'T' then 'Touring'
             else 'n/a'
        end as prd_line,
        cast(prd_start_dt as date) as prd_start_dt,
        cast(DATEADD(
            day,
            -1,
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            )
        ) as date) as prd_end_dt   -- data enrichment
        from bronze.crm_prd_info;
        set @end_time=getdate();
        print'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>---------------';


        set @start_time=getdate();
        print 'Truncating table: silver.crm_sales_details';
        truncate table silver.crm_sales_details;
        print 'Inserting into : silver.crm_sales_details';
        insert into silver.crm_sales_details (
               sls_ord_num,
               sls_prd_key,
               sls_cust_id,
               sls_order_dt,
               sls_ship_dt,
               sls_due_dt,
               sls_sales,
               sls_quantity,
               sls_price
            )
        select
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
             else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
             else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
             else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,
        case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price)
             then sls_quantity*ABS(sls_price)
             else sls_sales
        end as sls_sales,
        sls_quantity,
        case when sls_price is null or sls_price<=0
             then sls_sales/nullif(sls_quantity,0)
             else sls_price
        end as sls_price
        from bronze.crm_sales_details;
        set @end_time=getdate();
        print'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>---------------';



        set @start_time=getdate();
        print 'Truncating table: silver.erp_cust_az12';
        truncate table silver.erp_cust_az12;
        print 'Inserting into : silver.erp_cust_az12';
        insert into silver.erp_cust_az12(cid,bdate,gen)
        select
        case when cid like 'nas%' then SUBSTRING(cid,4,len(cid))
             else cid
        end as cid,
        case when bdate>GETDATE() then null
             else bdate
        end as bdate,
        case when upper(TRIM(gen)) in  ('F','FEMALE') THEN 'Female'
             when upper(TRIM(gen)) in  ('M','MALE') THEN 'Male'
             else 'n/a'
        end as gen
        from bronze.erp_cust_az12;
        set @end_time=getdate();
        print'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>---------------';


        set @start_time=getdate();
        print 'Truncating table: silver.erp_loc_a101';
        truncate table silver.erp_loc_a101;
        print 'Inserting into : silver.erp_loc_a101';
        insert into silver.erp_loc_a101(cid,cntry)
        select
        REPLACE(cid,'-','') cid,
        case when TRIM(cntry) = 'DE' then 'Germany'
             when TRIM(cntry) in ('US','USA') then 'United States'
             when TRIM(cntry) = '' or cntry is null then 'n/a'
             else TRIM(cntry)
        end as cntry
        from bronze.erp_loc_a101;
        set @end_time=getdate();
        print'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>---------------';


        set @start_time=getdate();
        print 'Truncating table: silver.erp_px_cat_g1v2';
        truncate table silver.erp_px_cat_g1v2;
        print 'Inserting into : silver.erp_px_cat_g1v2';
        insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        select 
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2;
        set @end_time=getdate();
        print'Load duration is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
        print'>---------------';

        set @batch_end_time=getdate();
        print'=============================================';
        print'loading silver layer is completed';
        print'Total load duration is'+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+'seconds'
        print'=============================================';
  end try
  begin catch
        print '================================================'
        print 'error occured during loading silver layer'
        print 'error message'+error_message();
        print 'error message'+cast(error_number() as nvarchar);
        print 'error message'+cast(error_state() as nvarchar);
        print '================================================'
  end catch
end


