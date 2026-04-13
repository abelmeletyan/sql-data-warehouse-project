/*
========================================================================================
Create Database and Schemas
========================================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver' and 'gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' databse if it exists.
  All data in the database will be permanently deleted. Proceed with caution
  and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE NAME = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
  
-- Create a 'Datawarehouse' database
create database Datawarehouse;

use Datawarehouse;
go

-- Schema is a folder or container that helps keep things organized.
-- GO is a separator that tells the application to fully execute one command before executing to the next command.
create schema bronze;
go
create schema silver;
go
create schema gold;
go
