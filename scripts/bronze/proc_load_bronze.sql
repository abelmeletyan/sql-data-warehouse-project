/*
=============================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/

-- use Datawarehouse;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
-- Load Data from source to the warehouse is BULK INSERT
-- Load CSV fild cust_info to crm_cust_info SQL table
BEGIN
	DECLARE @start_time datetime;
	DECLARE @end_time datetime;
	DECLARE @start_time_batch datetime;
	DECLARE @end_time_batch datetime;

	BEGIN TRY
		print('Loading Bronze Layer');
		PRINT('Full loading customer tables');

		set @start_time_batch = GETDATE();

		SET @start_time = GETDATE();
		print('>> Truncating table: bronze.crm_cust_info');
		truncate table bronze.crm_cust_info;

		print('>> Inserting data into table: bronze.crm_cust_info');
		-- Full Load is called when loading whole content of the file inside the table
		BULK insert bronze.crm_cust_info
		from 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_crm\cust_info.csv'
		with
		(
			FIRSTROW = 2,			-- Specify where the 2nd row starts
			FIELDTERMINATOR = ',',	-- Specify delimiter
			TABLOCK					-- Locks the table
		);
		SET @END_time = GETDATE();
		-- datediff(second, @start_time, @end_time);
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		-- select count(*) from bronze.crm_cust_info;

		-- -----------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		print('>> Truncating table: bronze.crm_prd_info');
		TRUNCATE TABLE BRONZE.CRM_PRd_INFO;

		print('>> Inserting data into table: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		from 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_crm\prd_info.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_time = GETDATE();
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		-- SELECT COUNT(*) FROM BRONZE.CRM_PRD_INFO;

		-- ----------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		print('>> Truncating table: bronze.crm_sales_details');
		TRUNCATE TABLE BRONZE.CrM_SALES_DETAILS;

		print('>> Inserting data into table: bronze.crm_sales_details');
		BULK INSERT BRONZE.CRM_SALES_DETAILS
		FROM 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_time = GETDATE();
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		-- SELECT COUNT(*) FROM BRONZE.crm_sales_details;

		-- ---------------------------------------------------------------------------------------------

		PRINT('Full loading ERP tables');
		SET @start_time = GETDATE();
		print('>> Truncating table: bronze.erp_cust_az12');
		TRUNCATE TABLE BRONZE.ERP_CUST_AZ12;

		print('>> Inserting data into table: bronze.erp_cust_az12');
		BULK INSERT BRONZE.ERP_CUST_AZ12
		FROM 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_time = GETDATE();
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		-- SELECT COUNT(*) FROM BRONZE.ERP_CUST_AZ12;

		-- -----------------------------------------------------------------------------------------------

		print('>> Truncating table: bronze.erp_loc_a101');
		SET @start_time = GETDATE();
		TRUNCATE TABLE BRONZE.ERP_LOC_A101;

		print('>> Inserting data into table: bronze.erp_loc_a101');
		BULK INSERT BRONZE.ERP_LOC_A101
		FROM 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_time = GETDATE();
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		-- SELECT COUNT(*) FROM BRONZE.ERP_LOC_A101;

		-- -----------------------------------------------------------------------------------------------

		print('>> Truncating table: bronze.erp_px_cat_g1v2');
		SET @start_time = GETDATE();
		TRUNCATE TABLE BRONZE.ERP_PX_CAT_G1V2;

		print('>> Inserting data into table: bronze.erp_px_cat_g1v2');
		BULK INSERT BRONZE.ERP_PX_CAT_G1V2
		FROM 'F:\EXTERNAL HARD DRIVE\_SQL__________\Data with Baraa_04.08.26\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_time = GETDATE();
		PRINT('>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		
		-- SELECT COUNT(*) FROM BRONZE.erp_px_cat_g1v2;

		set @end_time_batch = GETDATE();
		
		print('Loading of Bronze Layer is completed');
		print('Whole batch running time is ' + cast(datediff(second, @start_time_batch, @end_time_batch) as nvarchar) + ' seconds');
		
		
	END TRY
	BEGIN CATCH
		PRINT('Error occured during loading bronze layer');
		PRINT('Error message: ' + error_message());
		PRINT('Error number: ' + cast(error_number() as nvarchar));
		PRINT('Error number: ' + cast(error_state() as nvarchar));
	END CATCH
END;
