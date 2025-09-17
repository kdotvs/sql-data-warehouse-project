/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure silver.load_silver as

begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE()
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';
		
		set @start_time = GETDATE();
		print '>> Table 1: Truncation table: Silver.crm_cust_info';
		truncate table Silver.crm_cust_info;
		print '>> Table 1: Inserting data into: Silver.crm_cust_info';

		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim (cst_firstname) as cst_firstname,
		trim (cst_lastname) as cst_lastname,
		case 
			when upper(trim(cst_gndr)) = 'S' then 'Single'
			when upper(trim(cst_gndr)) = 'M' then 'Married'
			else 'n/a'
		end cst_material_status,
		case 
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end cst_gndr,
		cst_create_date
		from (
		SELECT *,
			ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			FROM [datawarehouse].[bronze].[crm_cust_info]
			where cst_id is not null
		)t where flag_last = 1
			
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @start_time = GETDATE()
		print '>> Table 2: Truncation table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Table 2: Inserting data into: Silver.crm_prd_info';

		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
		prd_id,
		replace(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,		  -- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost, -- Instead of null have zero for cot column
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a' 
		end as prd_line, -- Map product line codes to descriptive values
		cast (prd_start_dt as date) as prd_start_dt,
		cast(
			LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 
			as date
			) as prd_end_dt -- Calcualte end date as one day before the next start date
		FROM [datawarehouse].bronze.crm_prd_info;

		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @start_time = GETDATE();
		print '>> Table 3: Truncation table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>>Table 3: Inserting data into: silver.crm_sales_details';

		insert into silver.crm_sales_details (
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price 
		)

		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id ,
			case 
				when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case 
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt ,
			case 
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_due_dt ,
			case 
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
					then sls_quantity * ABS(sls_price)
				else sls_sales
			end as sls_sales,
			sls_quantity ,
			case 
				when sls_price is null or sls_price <= 0
					then sls_sales / nullif(sls_quantity, 0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details;

		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @start_time = GETDATE();
		print '>> Table 4: Truncation table: Silver.erp_cust_az12';
		truncate table Silver.erp_cust_az12;
		print '>>Table 4: Inserting data into: Silver.erp_cust_az12';

		insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)

		select 
			case 
				when CID like 'NAS%' then SUBSTRING(cid, 4, len(cid))
				else CID
			end as CID,
			case 
				when BDATE > GETDATE() then null
				else BDATE
			end as bdate,
			case 
				when upper(trim(GEN)) in ('F', 'Female') then 'Female'
				when upper(trim(GEN)) in ('M', 'Male') then 'Male'
				else 'n/a'
			end as gen
		from bronze.erp_cust_az12;

		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @start_time = GETDATE();
		print '>> Table 5: Truncation table: Silver.erp_loc_a101';
		truncate table Silver.erp_loc_a101;
		print '>>Table 5: Inserting data into: Silver.erp_loc_a101';

		insert into silver.erp_loc_a101
		(cid,
		cntry
		)
		select 
		REPLACE(cid, '-','') cid,
		case 
			when TRIM(cntry) = 'DE' then 'Germany'
			when TRIM(cntry) in ('US','USA') then 'United States'
			When TRIM(cntry) = '' or cntry is null then 'n/a'
			else TRIM(cntry)
		end as cntry
		from bronze.erp_loc_a101;

		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @start_time = GETDATE();
		print '>> Table 6: Truncation table: Silver.erp_px_cat_g1v2';
		truncate table Silver.erp_px_cat_g1v2;
		print '>>table 6: Inserting data into: Silver.erp_px_cat_g1v2';

		insert into silver.erp_px_cat_g1v2 
		(
		id,
		cat,
		subcat,
		maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;

		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		set @batch_end_time = GETDATE();
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		print 'Loading silver layer is completed';
		print ' - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

	end try
	begin catch
		print '=======================================================================';
		print 'Error occured during loadig silver layer';
		print 'Error Message' + Error_Message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '=======================================================================';
	end catch
end
