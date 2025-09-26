/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 


-- creating GOLD table
-- left join with tables

select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.BDATE,
	ca.GEN,
	la.CNTRY
from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ca
on 
	ci.cst_key = ca.CID
left join 
	silver.erp_loc_a101 la
on
	ci.cst_key = la.CID


-- To check the table has no duplicates write inside subquery


select cst_id, COUNT(*) as count_duplicates from
	(
		select 
			ci.cst_id,
			ci.cst_key,
			ci.cst_firstname,
			ci.cst_lastname,
			ci.cst_material_status,
			ci.cst_gndr,
			ci.cst_create_date,
			ca.BDATE,
			ca.GEN,
			la.CNTRY
		from 
			silver.crm_cust_info ci
		left join 
			silver.erp_cust_az12 ca
		on 
			ci.cst_key = ca.CID
		left join 
			silver.erp_loc_a101 la
		on
			ci.cst_key = la.CID
	) t group by cst_id

having count(*) > 1;


-- the final result has 2 gender columns so investigate and integrate the data

		select distinct
			ci.cst_gndr,
			ca.GEN
		from 
			silver.crm_cust_info ci
		left join 
			silver.erp_cust_az12 ca
		on 
			ci.cst_key = ca.CID
		left join 
			silver.erp_loc_a101 la
		on
			ci.cst_key = la.CID
		order by 1, 2

-- in the above scenario assume the data from the CRM table is the master table and accurate 


		select distinct
			ci.cst_gndr,
			ca.GEN,
			case 
				when ci.cst_gndr != 'n/a' then ci.cst_gndr
				else coalesce(ca.GEN, 'n/a')
			end as new_gen
		from 
			silver.crm_cust_info ci
		left join 
			silver.erp_cust_az12 ca
		on 
			ci.cst_key = ca.CID
		left join 
			silver.erp_loc_a101 la
		on
			ci.cst_key = la.CID
		order by 1, 2

-- implement the case statement in the original query

select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.GEN, 'n/a')
	end as new_gen,
	ci.cst_create_date,
	ca.BDATE,
	la.CNTRY
from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ca
on 
	ci.cst_key = ca.CID
left join 
	silver.erp_loc_a101 la
on
	ci.cst_key = la.CID;



-- Now alias the columns to the meaningful names

select 
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_material_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.GEN, 'n/a')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ca
on 
	ci.cst_key = ca.CID
left join 
	silver.erp_loc_a101 la
on
	ci.cst_key = la.CID;


-- The results of the above query are identified as DIMENSION table rather than FACT table
-- Now create surrogate key to make the records unique in this case using WINDOW function

select 
	ROW_NUMBER() over (order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_material_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.GEN, 'n/a')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ca
on 
	ci.cst_key = ca.CID
left join 
	silver.erp_loc_a101 la
on
	ci.cst_key = la.CID;


-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 

-- create GOLD DIM for products

select * from silver.crm_prd_info;

-- select columns & filter the latest record using NULL here and leave out the historical data

select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt
from
	silver.crm_prd_info pn
where prd_end_dt is null;

-- left Join with the other table 

select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	--pn.prd_end_dt,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
from
	silver.crm_prd_info pn
left join 
	silver.erp_px_cat_g1v2 pc
on
	pn.cat_id = pc.ID
where prd_end_dt is null;


-- Check the uniqueness or no duplicates on this join

select prd_key, COUNT(*) as prd_count 
from
	(
		select
			pn.prd_id,
			pn.cat_id,
			pn.prd_key,
			pn.prd_nm,
			pn.prd_cost,
			pn.prd_line,
			pn.prd_start_dt,
			pc.CAT,
			pc.SUBCAT,
			pc.MAINTENANCE
		from
			silver.crm_prd_info pn
		left join 
			silver.erp_px_cat_g1v2 pc
		on
			pn.cat_id = pc.ID
		where prd_end_dt is null
	) p group by prd_key
	having count(*) > 1;



-- Now bring like columns close together


select
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
from
	silver.crm_prd_info pn
left join 
	silver.erp_px_cat_g1v2 pc
on
	pn.cat_id = pc.ID
where prd_end_dt is null;


-- Give meaningfull names for the columns

select
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from
	silver.crm_prd_info pn
left join 
	silver.erp_px_cat_g1v2 pc
on
	pn.cat_id = pc.ID
where prd_end_dt is null;


-- Now identify the table as DIM or FACT, this one is DIM table hence create dim table
-- Also create surrogate key using WINDOW function to join with other tables 

select
	ROW_NUMBER () Over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from
	silver.crm_prd_info pn
left join 
	silver.erp_px_cat_g1v2 pc
on
	pn.cat_id = pc.ID
where prd_end_dt is null;


-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions

-- Gold Sales table

select * from silver.crm_sales_details;

-- Get all the columns

select 
	sd.sls_ord_num,
	sd.sls_prd_key,
	sd.sls_cust_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
from
	silver.crm_sales_details sd


-- Using the surrogate key from the created 2 dim tables, reaplace it in the sales table
-- use the dimension's surrogate keys instead of ID's to easily connect FACTS with DIM
-- use left join to join the date from dim table


select 
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	--sd.sls_prd_key,
	--sd.sls_cust_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
from
	silver.crm_sales_details sd
left join
	gold.dim_products pr
on
	sd.sls_prd_key = pr.product_number
left join 
	gold.dim_customers cu
on
	sd.sls_cust_id = cu.customer_id;


-- Give meaningful names

select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as duw_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from
	silver.crm_sales_details sd
left join
	gold.dim_products pr
on
	sd.sls_prd_key = pr.product_number
left join 
	gold.dim_customers cu
on
	sd.sls_cust_id = cu.customer_id;
