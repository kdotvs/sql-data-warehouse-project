/*
=============================================================
DDL Script : Create Brone Tables
=============================================================
Script Purpose:
    This script creates tables in the 'brone' schema, dropping existing tables. 
    If they already exist. 
   Run this script to re-define the DDL structure of 'brone' tables.
  'U' denotes the user has defined the tables under the schema.
*/

if object_id ('bronze.crm_cust_info' , 'U') is not null
	drop table bronze.crm_cust_info;

create table bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);
go

if object_id ('bronze.crm_sales_details' , 'U') is not null
	drop table bronze.crm_sales_details;

create table bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
go

if object_id ('bronze.crm_prd_info' , 'U') is not null
	drop table bronze.crm_prd_info;

create table bronze.crm_prd_info(
prd_id	int,
prd_key	NVARCHAR(50),
prd_nm	NVARCHAR(50),
prd_cost int,
prd_line NVARCHAR(50),
prd_start_dt datetime,
prd_end_dt datetime
);
go


if object_id ('bronze.erp_cust_az12' , 'U') is not null
	drop table bronze.erp_cust_az12;

create table bronze.erp_cust_az12 (
CID NVARCHAR(50),
BDATE date,
GEN NVARCHAR(50)
);
go


if object_id ('bronze.erp_loc_a101' , 'U') is not null
	drop table bronze.erp_loc_a101;

create table bronze.erp_loc_a101 (
CID	NVARCHAR(50),
CNTRY NVARCHAR(50)
);
go

if object_id ('bronze.erp_px_cat_g1v2' , 'U') is not null
	drop table bronze.erp_px_cat_g1v2;

create table bronze.erp_px_cat_g1v2 (
ID	NVARCHAR(50),
CAT	NVARCHAR(50),
SUBCAT	NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);
go
