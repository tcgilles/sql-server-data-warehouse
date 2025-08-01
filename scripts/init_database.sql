/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'SalesDwh' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'SalesDwh' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'SalesDwh' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SalesDwh')
BEGIN
    ALTER DATABASE SalesDwh SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SalesDwh;
END;
GO

-- Create the 'SalesDwh' database
CREATE DATABASE SalesDwh;
GO

USE SalesDwh;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
