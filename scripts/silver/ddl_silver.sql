/*
====================================================================================
DDL Script: Create Silver Tables
====================================================================================
Script Purpose:
  This script creates tables in the 'silver' schema, dropping existing tables
    if they already exist.
  Run This script to re-define the DDL structure of 'bronze' tables
*/

-- SILVER LAYER
-- Create tables
-- Ctrl + H --> Find and Replace (replace: bronze to silver)

-- Session time with Instructor - #1
-- Step: #1 - Silver Layer
-- Date: 04/16/26

-- Replace 'silver' to 'silver'
if OBJECT_ID ('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info;

-- Create table silver.crm_cust_info
create table silver.crm_cust_info
(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;

-- Create table silver.crm_prd_info
create table silver.crm_prd_info
(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;

-- Create table silver.crm_sales_details
create table silver.crm_sales_details
(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;

-- Create table silver.erp_cust_az12
create table silver.erp_cust_az12
(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_loc_a101', 'U') is not null
	drop table silver.erp_loc_a101;

-- Create table silver.erp_loc_a101
create table silver.erp_loc_a101
(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2;

-- Create table silver.erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2
(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
