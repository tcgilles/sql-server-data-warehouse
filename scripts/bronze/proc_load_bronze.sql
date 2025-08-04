/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
*/
USE SalesDwh
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
===============================================================================
Author:      Gilles Takam Cheno	
Create Date: 04/08/2025
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    - @directpry_path VARCHAR(100):
	  The base directory where the files are located. Only works on Windows systems.
	  Example : 'C:\project\'
    - @log_to_table BIT: Optional
      Wether or not to save the logs of the process
      Default value is 1

Usage Example:
    EXEC bronze.load_bronze 'C:\project\';
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze (
    @directory_path VARCHAR(170),
    @log_to_table BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @sql                NVARCHAR(4000),
        @crm_directory      VARCHAR(200),
        @erp_directory      VARCHAR(200),
        @file_path          VARCHAR(250),
        @error_file_path    VARCHAR(250),
        @file_exists        INT,
        @start_time         DATETIME,
        @end_time           DATETIME,
        @batch_start_time   DATETIME,
        @batch_end_time     DATETIME,
        @row_count          INT,
        @error_message      NVARCHAR(4000),
        @error_number       INT,
        @error_state        INT;

    -- Ensure directory path ends with backslash
    SET @directory_path = RTRIM(@directory_path);
    IF RIGHT(@directory_path, 1) != '\'
        SET @directory_path = @directory_path + '\';

    SET @crm_directory = @directory_path + 'source_crm\';
    SET @erp_directory = @directory_path + 'source_erp\';

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- Begin transaction for atomicity
        BEGIN TRANSACTION;

        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        -- Load CRM Tables
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        SET @file_path = @crm_directory + 'cust_info.csv'
        SET @error_file_path = @crm_directory + 'error_crm_cust_info.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.crm_cust_info';
            SET @sql = N'
            BULK INSERT bronze.crm_cust_info
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.crm_cust_info);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.crm_cust_info', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        -- Load crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        SET @file_path = @crm_directory + 'prd_info.csv'
        SET @error_file_path = @crm_directory + 'error_crm_prd_info.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.crm_prd_info';
            SET @sql = N'
            BULK INSERT bronze.crm_prd_info
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.crm_prd_info);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.crm_prd_info', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        -- Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        SET @file_path = @crm_directory + 'sales_details.csv'
        SET @error_file_path = @crm_directory + 'error_crm_sales_details.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.crm_sales_details';
            SET @sql = N'
            BULK INSERT bronze.crm_sales_details
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.crm_sales_details);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.crm_sales_details', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        -- Load ERP Tables
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- Load erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        SET @file_path = @erp_directory + 'CUST_AZ12.csv'
        SET @error_file_path = @erp_directory + 'error_erp_cust_az12.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
            SET @sql = N'
            BULK INSERT bronze.erp_cust_az12
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.erp_cust_az12);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.erp_cust_az12', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        -- Load erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        SET @file_path = @erp_directory + 'LOC_A101.csv'
        SET @error_file_path = @erp_directory + 'erp_loc_a101.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
            SET @sql = N'
            BULK INSERT bronze.erp_loc_a101
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.erp_loc_a101);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.erp_loc_a101', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        -- Load erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        SET @file_path = @erp_directory + 'PX_CAT_G1V2.csv'
        SET @error_file_path = @erp_directory + 'erp_px_cat_g1v2.csv'
        EXEC master.dbo.xp_fileexist @file_path, @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
            SET @sql = N'
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM ''' + @file_path + '''
            WITH (
                FORMAT = ''CSV'',
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                ERRORFILE = ''' + @error_file_path + ''',
                TABLOCK
            );';
            EXEC sp_executesql @sql;
            SET @end_time = GETDATE();
            SET @row_count = (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
            IF @log_to_table = 1
                INSERT INTO bronze.load_log (TableName, Operation, [RowCount], DurationSeconds, [Status])
                VALUES ('bronze.erp_px_cat_g1v2', 'BULK INSERT', @row_count, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
            PRINT '>> -------------';
        END

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(ROUND(DATEDIFF(SECOND, @batch_start_time, @batch_end_time), 1) AS VARCHAR(256)) + ' seconds';
        PRINT '==========================================';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        -- Capture error details
        SET @error_message = ERROR_MESSAGE();
        SET @error_number = ERROR_NUMBER();
        SET @error_state = ERROR_STATE();

        -- Log error
        IF @log_to_table = 1
            INSERT INTO bronze.load_log (TableName, Operation, [Status], ErrorMessage, LogDate)
            VALUES ('bronze_layer', 'FULL LOAD', 'Failed', @error_message + ' (Error Number: ' + CAST(@error_number AS NVARCHAR) + ', State: ' + CAST(@error_state AS NVARCHAR) + ')', GETDATE());

        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + @error_message;
        PRINT 'Error Number: ' + CAST(@error_number AS NVARCHAR);
        PRINT 'Error State: ' + CAST(@error_state AS NVARCHAR);
        PRINT '==========================================';

        -- Re-throw error to caller
        THROW;
    END CATCH
END;
