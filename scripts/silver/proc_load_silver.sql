/*
====================================================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
====================================================================================
Script purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
  - Truncates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  - None
  - This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC silver.load_silver;
*/

-- =================================================================================
-- Combine ALL Silver Layer queries
-- =================================================================================

USE [WarehouseProject_Baraa]
GO
/****** Object:  StoredProcedure [Bronze].[load_bronze]    Script Date: 4/21/2026 1:22:58 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
DECLARE @start_time datetime, @end_time datetime;
DECLARE @batch_start_time datetime, @batch_end_time datetime;

BEGIN TRY
	-- Table #1
	PRINT '=====================================================================';
	PRINT 'Loading Silver Layer'
	PRINT '=====================================================================';

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loading crm_cust_info table'
	PRINT '---------------------------------------------------------------------';
	
	SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	print 'Truncating table: silver.crm_cut_info';
	TRUNCATE TABLE silver.crm_cust_info
	print '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	select
		cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
		case	
			when UPPER(trim(cst_marital_status)) = 'M' then 'Married'
			when UPPER(trim(cst_marital_status)) = 'S' then 'Single'
			else 'N/A' end cst_marital_status,
		case	
			when UPPER(trim(cst_gndr)) = 'M' then 'Male'
			when UPPER(trim(cst_gndr)) = 'F' then 'Female'
			else 'N/A' end cst_gndr,
		cst_create_date
	from 
	(
		select *,
			row_number() over(partition by cst_id order by cst_create_date desc) dup_count
		from bronze.crm_cust_info
	)d
	where d.dup_count = 1 and cst_id is not null;
	SET @end_time = GETDATE()
	print('Load time for silver.crm_cust_info is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loading crm_prd_info table'
	PRINT '---------------------------------------------------------------------';
	
	-- Table #2
	SET @start_time = GETDATE();
	print 'Truncating table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info
	print '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info
	(
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
		replace(substring(prd_key, 1, 5), '-','_') cat_id,
		substring(prd_key, 7, len(prd_key)) prd_key,
		trim(prd_nm) prd_nm,
		ISNULL(prd_cost,0) prd_cost,
		case
			when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
			when UPPER(TRIM(prd_line)) = 'R' then 'Road'
			when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
			when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
			else 'N/A' end prd_line,
		cast(prd_start_dt as date) prd_start_dt,
		cast(dateadd(day, -1, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) AS prd_end_dt
	from bronze.crm_prd_info;
	SET @end_time = GETDATE()
	print('Load time for silver.crm_prd_info is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loading crm_sales_details table'
	PRINT '---------------------------------------------------------------------';

	-- Table #3
	SET @start_time = GETDATE();
	print 'Truncating table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	print '>> Inserting Data Into: silver.crm_sales_details';
	insert into silver.crm_sales_details
	(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case	
			when sls_order_dt=0 or len(sls_order_dt) != 8 then NULL
			else cast(cast(sls_order_dt as nvarchar) as date)
		end sls_order_dt,
		case	
			when sls_ship_dt=0 or len(sls_ship_dt) != 8 then NULL
			else cast(cast(sls_ship_dt as varchar) as date)
		end sls_ship_dt,
		case	
			when sls_due_dt=0 or len(sls_due_dt) != 8 then NULL
			else cast(cast(sls_due_dt as varchar) as date) 
		end sls_due_dt,
		case
			when sls_sales is null or sls_sales <= 0 or sls_sales != (sls_quantity * ABS(sls_price))
				then sls_quantity * ABS(sls_price)
			else sls_sales
		end sls_sales,
		sls_quantity,
		case
			when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price
		end as sls_price
	from bronze.crm_sales_details;
	SET @end_time = GETDATE()
	print('Load time for silver.crm_sales_details is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loading erp_cust_az12 table'
	PRINT '---------------------------------------------------------------------';

	-- Table #4
	SET @start_time = GETDATE();
	print 'Truncating table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12
	print '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12
	(cid, bdate, gen)
	select
		-- Get the substring of CID
		case
			when cid LIKE 'NAS%' then substring(cid, 4, len(cid))		-- Correct
			else cid
		end cid,
		-- Check if birth dates is out of range
		case 
			when bdate < '1924-01-01' or bdate > getdate() then NULL
			else bdate
		end bdate,
		-- Standardize gender
		case when upper(trim(gen)) IN ('M', 'MALE') then 'Male'
			when upper(trim(gen)) = 'F' then 'Female'
		when upper(trim(gen)) = ' ' or gen is null then 'n/a'
		else gen
	end gen
	from bronze.erp_cust_az12;
	SET @end_time = GETDATE()
	print('Load time for silver.erp_cust_az12 is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loading erp_loc_a101 table'
	PRINT '---------------------------------------------------------------------';

	-- Table #5
	SET @start_time = GETDATE();
	print 'Truncating table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101
	print '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	(cid, cntry)
	SELECT
		replace(cid, '-','') cid,
		case 
			when trim(cntry) = 'DE' then 'Germany' 
			when trim(cntry)='' or cntry is null then 'n/a'
			when trim(cntry) IN ('USA', 'US') then 'United States'
			else trim(cntry)
		end cntry
	from bronze.erp_loc_a101;
	SET @end_time = GETDATE()
	print('Load time for silver.erp_loc_a101 is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	PRINT '---------------------------------------------------------------------';
	PRINT 'Loadingerp_px_cat_g1v2'
	PRINT '---------------------------------------------------------------------';

	-- Table #6
	SET @start_time = GETDATE();
	print 'Truncating table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	SELECT
		TRIM(id) id,
		trim(cat) cat,
		trim(subcat) subcat,
		trim(maintenance) maintenance
	from bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE()
	print('Load time for silver.erp_px_cat_g1v2 is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds!')

	SET @batch_end_time = GETDATE()
	print('Load time for for the whole batch is ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' second(s).');

END TRY
BEGIN CATCH
	PRINT 'Error occured during loading silver layer';
	PRINT 'Error Message: ' + error_message();
	PRINT 'Error Number: ' + cast(error_number() as nvarchar);
	PRINT 'Error State: ' + cast(error_state() as nvarchar);
END CATCH
END;

-- EXEC SILVER.LOAD_SILVER;
