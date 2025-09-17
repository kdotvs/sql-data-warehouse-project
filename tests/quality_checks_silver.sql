-- Check data quality (Invalid date order)

select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- check data consistency: betweeen sales, quantity, & price
-- >> sales = quantity * price
-- >> Values must not be NULL, Zero or negative.


select distinct
	sls_sales ,
	sls_quantity ,
	sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

select * from silver.crm_sales_details;
