EXEC bronze.load_bronze;


create or alter procedure bronze.load_bronze as
	begin
		declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
		begin try
			set @batch_start_time = GETDATE();
			PRINT '================================================================';
			PRINT 'Loading Bronze Layer'
			PRINT '================================================================';

			PRINT '________________________________________________________________';
			PRINT 'loading CRM tables';
			PRINT '________________________________________________________________';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_cust_info';
			truncate table bronze.crm_cust_info;

			PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
			bulk insert bronze.crm_cust_info
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_crm\cust_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.crm_cust_info;
			set @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_prd_info';
			truncate table bronze.crm_prd_info;

			PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
			bulk insert bronze.crm_prd_info
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_crm\prd_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.crm_prd_info;
			set @end_time = GETDATE()
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_Time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_sales_details';
			truncate table bronze.crm_sales_details;

			PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
			bulk insert bronze.crm_sales_details
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_crm\sales_details.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.crm_sales_details;
			set @end_time = GETDATE()
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_Time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';


			PRINT '________________________________________________________________';
			PRINT 'loading ERP tables';
			PRINT '________________________________________________________________';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_cust_az12';
			truncate table bronze.erp_cust_az12;

			PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
			bulk insert bronze.erp_cust_az12
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_erp\CUST_AZ12.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.erp_cust_az12;
			set @end_time = GETDATE()
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_Time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_loc_a101';
			truncate table bronze.erp_loc_a101;

			PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
			bulk insert bronze.erp_loc_a101
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_erp\LOC_A101.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.erp_loc_a101;
			set @end_time = GETDATE()
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_Time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';

			set @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
			truncate table bronze.erp_px_cat_g1v2;

			PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
			bulk insert bronze.erp_px_cat_g1v2
			from 'D:\WorkSpace\Sql_Projects\DataWarehouse\source_erp\PX_CAT_G1V2.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

			--select count(*) from bronze.erp_px_cat_g1v2;
			set @end_time = GETDATE()
			PRINT '>> Load Duration: ' + cast(datediff(second, @start_Time, @end_time) as nvarchar) + ' seconds';

			PRINT '----------------------------------------------------------------';
		set @batch_end_time = GETDATE();
		print '====================================================================';
		print 'Loading Bronze Layer is Completed';
		print '<< Batch load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		end try
	begin catch
		PRINT '=======================================================================';
		PRINT ' ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================================';
	end catch
	end;
