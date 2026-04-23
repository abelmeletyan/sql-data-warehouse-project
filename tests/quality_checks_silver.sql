/*
===================================================================================
Quality Checks
===================================================================================
Script Purpose:
  This script perfomrs various quality checks for data consistency, accuracy and
  standardization across the 'silver' schemas. It includes cheks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading silver layers.
  - Investigate and resolve any discripancies found during the checks.
===================================================================================
*/

-- GitHub

-- Table #1
-- =================================================================================
-- Silver Layer - bronze.crm_cust_info                                 =============
-- =================================================================================

use WarehouseProject_Baraa;

-- Clean and Load

select * from bronze.crm_cust_info
where len(cst_gndr) > 1
order by cst_marital_status desc;

select *
from
(
	select *,
		row_number() over(partition by cst_id order by cst_create_date desc) dup_count
	from bronze.crm_cust_info
)d
where d.dup_count = 1;

-- ===================================================================
-- Check for unwanted sapces
-- Expectation: No Results
-- Trim spaces

select
	cst_lastname
from bronze.crm_cust_info
-- where LEN(cst_firstname) != len(trim(cst_firstname))
-- OR
where cst_lastname != trim(cst_lastname)
;

-- =================================================================================
-- Query completes following
	-- Removes primary key duplicates
	-- Removes spaces from strings

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

-- ============================================================================

-- Check new table

select *
from
(
	select *,
		row_number() over(partition by cst_id order by cst_create_date desc) dup_count
	from silver.crm_cust_info
)d
where d.dup_count > 1;

select
	cst_lastname
from silver.crm_cust_info
-- where LEN(cst_firstname) != len(trim(cst_firstname))
-- OR
where cst_lastname != trim(cst_lastname)
;

select distinct
	cst_marital_status
from silver.crm_cust_info;

select * from silver.crm_cust_info;

-- Table #2
-- ========= MAIN QUERY - TABLE #2 (BRONZE.CRM_PRD_INFO) ==================================================================

select
	prd_id,
	prd_key,
	-- left(prd_key, 5) prd_key_1,
	replace(substring(prd_key, 1, 5), '-','_') cat_id,
	substring(prd_key, 7, len(prd_key)) prd_key,
	trim(prd_nm) prd_nm,
	ISNULL(prd_cost,0) prd_cost,
	-- prd_line,
	case
		when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
		when UPPER(TRIM(prd_line)) = 'R' then 'Road'
		when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
		when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
		else 'N/A' end prd_line,
	cast(prd_start_dt as date) prd_start_dt,
	-- prd_end_dt,
	cast(dateadd(day, -1, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) AS prd_end_dt
from bronze.crm_prd_info;


-- ========= CLEANSING PROCESS =================================================================

-- Table #2
-- Check for primary key duplicates or nulls

select
	prd_id,
	count(*) record_count
from bronze.crm_prd_info
group by prd_id
having count(*) < 1 or prd_id is null;

-- -----------------------------------------------------------------------------------
-- Check for extra spaces (trim)
select
	prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- -------------------------------------------------------------------------------------

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
-- Solution #1: Switch End Date and Start Date
	-- Issue: The dates are overlapping!!!
	-- Issue: Null values for dates. Each record must have a Start Date! It's ok to have NULL values for End Dates
-- Solution #2a: Derive the End Date from the Start Date
	-- Solution #2b. End Date = Start Date of the next record -1. Use Dateadd() function.
select * from bronze.crm_prd_info
where prd_start_dt > prd_end_dt;

-- Data Standardization & Consistency
-- Check all possible values of prd_line
select distinct prd_line 
from bronze.crm_prd_info;

-- Check for NULLs or negative numbers
select prd_cost from bronze.crm_prd_info
where prd_cost is null or prd_cost < 0;

-- Check if substring equals or not equals to a string
-- where substring(prd_key, 7, len(prd_key)) not in
--	(select sls_prd_key from bronze.crm_sales_details);

-- where replace(substring(prd_key, 1, 5), '-','_')
-- not in (select distinct id from bronze.erp_px_cat_g1v2);

-- Split prd_key column in 2 columns

select * from bronze.crm_prd_info;

--=============================================================================================

select * from silver.crm_cust_info;

select * from silver.crm_prd_info;

-- ============ UPDATE TABLE STRUCTURE ============================================================

if OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;

-- Create table silver.crm_prd_info
create table silver.crm_prd_info
(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);

-- ========== INSERT DATA INTO TABLE =============================================================

insert into silver.crm_prd_info
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

select * from silver.crm_prd_info;


-- ======== QUALITY CHECK =========================================================================
-- Quality Checks
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
select
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces
-- Expectation: No Results
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
select prd_cost
from silver.crm_prd_info
where prd_cost < 0;

-- Data Standardization & Consistency
select distinct prd_line
from silver.crm_prd_info;

-- Check for Invalid Dates
select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt;

-- Table #3
-- ======================================================================================
-- ========= MAIN QUERY - TABLE #3 (BRONZE.CRM_SALES_DETAILS)        ====================
-- ======================================================================================

SELECT * FROM BRONZE.CRM_SALES_DETAILS;


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
	-- I don't know if ok to transform. Better choice is talk to experts (steakholders, business dept, source system).
	-- Discuss and find out how to proceed. Usually 2 answers: 
		-- 1) Solution #1: Somebody will fix the data in the source. Data Issues will be fixed directly in source system
		-- 2) Solution #2: Data Issues has to be fixed in data warehouse. I will be asked to leave the data as is 
		   -- or remove for the quality of data.
	-- Rules:   1) If sales is negative, zero or null then derive it using Quantity and price.
		--	2) If price is zero or null then calculate it using Sales and Quantity.
		--  3) If price is negative, convert it to a positive value
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




----------- CHECK FOR QUALITY AND TRANSFORMATIONS STEPS -------------------------------------------------------
-- Check if primary keys are duplicated and if NULLs

select
	sls_cust_id,
	count(*)
from bronze.crm_sales_details
group by sls_cust_id
having count(*) > 1 and sls_cust_id is null;

-- Check for extra spaces
select
	sls_prd_key
from bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key);

-- Check if sls_prd_key (bronze.crm_sales_details) are in table silver.crm_prd_info) and can be used for analysis
-- Expectation: No Result
select
	sls_prd_key
from bronze.crm_sales_details
where sls_prd_key NOT IN (select sls_prd_key from silver.crm_prd_info);

-- Check if sls_cust_id (bronze.crm_sales_details) are in table silver.crm_cust_info) and can be used for analysis
-- Expectation: No Result
select
	sls_cust_id
from bronze.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info);

-- Check for Invalid Dates
	-- Negative numbers
	-- Zeros
select
	nullif(sls_order_dt, 0) sls_order_dt,
	sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
	or len(sls_order_dt) != 8
	or sls_order_dt > 20500101
	or sls_order_dt < 19000101;

-- Order Date must always be earlier than the Shipping Date or Due Date

-- Check data consistency between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative

select
	sls_ord_num,
	sls_sales,
	sls_quantity,
	sls_price,
	sls_price * sls_quantity AS sls_sales_updated,
	case
		when (sls_price * sls_quantity) <= 0 or (sls_price * sls_quantity) is NULL
			then sls_sales / sls_quantity
		else sls_price * sls_quantity
	end AS sls_sales_2,
	case
		-- Rules:   1) If sales is negative, zero or null then derive it using Quantity and price.
				--	2) If price is zero or null then calculate it using Sales and Quantity.
				--  3) If price is negative, convert it to a positive value
		when sls_sales is null or sls_sales <= 0 then sls_quantity * sls_price
		when sls_price = 0 or sls_price is null then sls_sales / sls_quantity
		when sls_price < 0 then abs(sls_price)
		else sls_price * sls_quantity
		end sls_sales_3
from bronze.crm_sales_details
where sls_sales != (sls_price * sls_quantity)
	or (sls_price * sls_quantity) <= 0
	or (sls_price * sls_quantity) is null
order by sls_sales, sls_quantity, sls_price;

-- -----------------------------------------------

select
	sls_ord_num,
	sls_sales,
	sls_quantity,
	sls_price,
	sls_price * sls_quantity AS sls_sales_updated
from bronze.crm_sales_details
where sls_sales != (sls_price * sls_quantity)
	or sls_price is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- ------------------------------------

-- =========================================================================
-- Quality Check for Query #3 ENDS HERE                      ===============
-- =========================================================================

-- Check DDL before inserting data into silver.crm_sales_details.

if OBJECT_ID('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;

-- UPDATE table silver.crm_sales_details
create table silver.crm_sales_details
(
	sls_ord_num nvarchar(50),		-- Good
	sls_prd_key nvarchar(50),		-- Good
	sls_cust_id int,		-- Good
	sls_order_dt date,		-- Replaced int to date
	sls_ship_dt date,		-- Replaced int to date
	sls_due_dt date,		-- Replaced int to date
	sls_sales int,		-- Good
	sls_quantity int,		-- Good
	sls_price int,		-- Good
	dwh_create_date datetime2 default getdate()
);

-- Insert updated data into silver.crm_sales_details

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

-- Extract data from silver.crm_sales_details;

select * from silver.crm_sales_details;

-- ==============================================================================================
-- ========== Check Quality of Silver table now =================================================
-- ==============================================================================================

select
	sls_cust_id,
	count(*)
from silver.crm_sales_details
group by sls_cust_id
having count(*) > 1 and sls_cust_id is null;

-- Check for extra spaces
select
	sls_prd_key
from silver.crm_sales_details
where sls_prd_key != trim(sls_prd_key);

-- Check if sls_prd_key (bronze.crm_sales_details) are in table silver.crm_prd_info) and can be used for analysis
-- Expectation: No Result
select
	sls_prd_key
from bronze.crm_sales_details
where sls_prd_key NOT IN (select sls_prd_key from silver.crm_prd_info);

-- Check if sls_cust_id (bronze.crm_sales_details) are in table silver.crm_cust_info) and can be used for analysis
-- Expectation: No Result
select
	sls_cust_id
from bronze.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info);

-- Check for Invalid Dates
	-- Negative numbers
	-- Zeros
select
	nullif(sls_order_dt, 0) sls_order_dt,
	sls_order_dt
from silver.crm_sales_details
where sls_order_dt <= 0 
	or len(sls_order_dt) != 8
	or sls_order_dt > 20500101
	or sls_order_dt < 19000101;

-- Check for Invalid Date orders
-- Expectation: No Result
select *
from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Order Date must always be earlier than the Shipping Date or Due Date

-- Check data consistency between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative

-- Expectation: No Result
select
	sls_ord_num,
	sls_sales,
	sls_quantity,
	sls_price,
	sls_price * sls_quantity AS sls_sales_updated,
	case
		when (sls_price * sls_quantity) <= 0 or (sls_price * sls_quantity) is NULL
			then sls_sales / sls_quantity
		else sls_price * sls_quantity
	end AS sls_sales_2,
	case
		-- Rules:   1) If sales is negative, zero or null then derive it using Quantity and price.
				--	2) If price is zero or null then calculate it using Sales and Quantity.
				--  3) If price is negative, convert it to a positive value
		when sls_sales is null or sls_sales <= 0 then sls_quantity * sls_price
		when sls_price = 0 or sls_price is null then sls_sales / sls_quantity
		when sls_price < 0 then abs(sls_price)
		else sls_price * sls_quantity
		end sls_sales_3
from silver.crm_sales_details
where sls_sales != (sls_price * sls_quantity)
	or (sls_price * sls_quantity) <= 0
	or (sls_price * sls_quantity) is null
order by sls_sales, sls_quantity, sls_price;

-- -----------------------------------------------

select
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where sls_sales != (sls_price * sls_quantity)
	or sls_price is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

select * from silver.crm_sales_details;

-- Table #4
-- =============================================================================
-- ==== Silver Layer - erp_cust_az12                                  ==========
-- =============================================================================

use WarehouseProject_Baraa;

-- ******* Check format of a date (bdate like xxxx-xx-xx) 

select * from bronze.erp_cust_az12;

--==============================================================================
-- ==== MAIN QUERY - Silver Layer - erp_cust_az12                     ==========

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

-- =============================================================================
-- Quality check begins here 

-- Trim spaces
select cid
from bronze.erp_cust_az12
where cid != trim(cid);

-- Check if all CIDs the same length
select distinct len(cid)
from bronze.erp_cust_az12;


select bdate
from bronze.erp_cust_az12
where len(bdate) != 10;

-- ----------------------------------------------------
-- Check if 'gender is standardized.
select distinct
	gen
from bronze.erp_cust_az12;

-- NULL: 1471
select
	gen
from bronze.erp_cust_az12
where gen is null;

-- M or F: 9
select
	gen
from bronze.erp_cust_az12
where gen IN ('M','F');

-- Blank: 4
select
	gen
from bronze.erp_cust_az12
where gen = ' ';

-- Check Gender -------------------------------------------------------
select
	cid,
	bdate,
	gen,
	case 
		when gen = 'M' then 'Male'
		when gen = 'F' then 'Female'
		when gen = ' ' or gen is null then 'N/A'
		else gen
	end gen_2
from bronze.erp_cust_az12
where gen is null
	or gen in ('M', 'F')
	or gen = ' ';

-- --------------------------------------------------------------

select * from silver.crm_cust_info;
select * from bronze.erp_cust_az12;

select distinct len(cid), substring_cid_start
from
(
	select
		cid,
		reverse(left(reverse(cid), 10)) reverse_cid,
		substring(cid, 1, 3) substring_cid_start,
		substring(cid, 4, len(cid)) substring_cid_end
	from bronze.erp_cust_az12
)d
order by substring_cid_start;

-- Check CID     ---------------------------------------------------------------

select *
-- distinct left(new_cid,2)
from
(
	select
		cid,
		case
			when cid LIKE 'NAS%' then substring(cid, 4, len(cid))		-- Correct
			else cid
		end new_cid_instuctor,
		case
			when left(cid, 3) = 'NAS' then substring(cid, 4, len(cid))		-- Correct
			else cid
		end new_cid,	
		reverse(left(reverse(cid), 10)) reverse_cid,						-- Correct
		substring(cid, 1, 3) substring_cid_start,							-- Incorrect
		substring(cid, 4, len(cid)) substring_cid_end						-- Incorrect
	from bronze.erp_cust_az12
) d
where new_cid_instuctor NOT IN (select cst_key from silver.crm_cust_info);
-- where len(new_cid) = 10 and left(new_cid,2)='AW';
-- where new_cid != reverse_cid;

-- Check bdate ------------------------------------------------------------------
	-- Check if bdate is less than '1900-01-01' or greater than today's date

select * from silver.crm_cust_info;

select * from bronze.erp_cust_az12;

select * 
from bronze.erp_cust_az12
where bdate < '1924-01-01'
	or bdate > getdate();

-- ======================================================================
-- Insert the data into silver.erp_cust_az12
  -- No need to update DDL as data type in the table has not been changed.

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

---------------------------------------------------------------------

-- Check CID that starts with 'AW' and cid length is 10
select * 
from silver.erp_cust_az12
where cid like 'AW%' and len(cid) =10;

-- Check if date of birth is not out of range
select * 
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- Check all distinct values of gender.
select distinct gen 
from silver.erp_cust_az12;


-- =============================================================================
-- ==== Silver Layer - erp_loc_a101                                   ==========
-- =============================================================================

use WarehouseProject_Baraa;

select * from silver.crm_cust_info;

-- Total rows18,484
select * from bronze.erp_loc_a101;

-- ===== MAIN QUERY ==========================================================

-- Replace '-' in CID with ''
SELECT
	replace(cid, '-','') cid,
	case 
		when trim(cntry) = 'DE' then 'Germany' 
		when trim(cntry)='' or cntry is null then 'n/a'
		when trim(cntry) IN ('USA', 'US') then 'United States'
		else trim(cntry)
	end cntry
from bronze.erp_loc_a101;



-- ===== Quality Check =======================================================
select *
from bronze.erp_loc_a101
where cid like '%-%';

-- Replace '-' in CID with ''
SELECT
	replace(cid, '-','') cid_2
from bronze.erp_loc_a101
where replace(cid, '-','') NOT IN (select cst_key from silver.crm_cust_info);

-- Check data Standardization & Consistency
-- Trim
-- Replace unidentified countries and Blank values to NULL
SELECT distinct
	cntry,
	case 
		when trim(cntry) = 'DE' then 'Germany' 
		when trim(cntry)='' or cntry is null then 'n/a'
		when trim(cntry) IN ('USA', 'US') then 'United States'
		else trim(cntry)
	end cntry_2
from bronze.erp_loc_a101;

-- Insert into silver.erp_loc_a101 -------------------------------------------------------------

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

-- Check Final Table -------------------------------------------------------------------

select cid
from silver.erp_loc_a101;


-- =============================================================================
-- ==== Silver Layer - erp_px_cat_g1v2                                ==========
-- =============================================================================

use WarehouseProject_Baraa;

select * from silver.crm_prd_info;

select * from bronze.erp_px_cat_g1v2;

-- ===== MAIN QUERY =============================================================

SELECT
	TRIM(id) id,
	trim(cat) cat,
	trim(subcat) subcat,
	trim(maintenance) maintenance
from bronze.erp_px_cat_g1v2;

-- ==============================================================================

-- Quality Check
select
	id
from bronze.erp_px_cat_g1v2
where id like '%_%';

-- Check 1
SELECT
	TRIM(id) id_2
from bronze.erp_px_cat_g1v2;

-- Check 2

SELECT distinct
	trim(cat)
from bronze.erp_px_cat_g1v2;

-- Check 3

SELECT
	trim(subcat)
from bronze.erp_px_cat_g1v2
where subcat != trim(subcat);

-- Check 4

SELECT
	trim(maintenance)
from bronze.erp_px_cat_g1v2
where maintenance!=trim(maintenance);
