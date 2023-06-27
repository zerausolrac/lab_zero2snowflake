/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Instalación
Version:      v1
Script:       TastyBytes_LATAM_Instalacion.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/

USE ROLE sysadmin;

/*--
 Creación de bases de datos y esquemas
--*/

-- crear base de datos latam_frostbyte_tasty_bytes 
CREATE OR REPLACE DATABASE latam_frostbyte_tasty_bytes;
-- crear esquema  raw_pos 
CREATE OR REPLACE SCHEMA latam_frostbyte_tasty_bytes.raw_pos;
-- crear esquema  raw_customer 
CREATE OR REPLACE SCHEMA latam_frostbyte_tasty_bytes.raw_customer;
-- crear esquema  harmonized 
CREATE OR REPLACE SCHEMA latam_frostbyte_tasty_bytes.harmonized;
-- crear esquema  analytics 
CREATE OR REPLACE SCHEMA latam_frostbyte_tasty_bytes.analytics;




-- creción de wharehouse 
CREATE OR REPLACE WAREHOUSE latam_build_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'warehouse demo para Latam testybyte';


alter warehouse latam_build_wh set warehouse_size = 'xxxlarge' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Creacion de roles
USE ROLE securityadmin;

-- Creación de  roles funcionales
CREATE ROLE IF NOT EXISTS latam_tasty_admin
    COMMENT = 'admin para latam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_data_engineer
    COMMENT = 'data engineer para latam tasty bytes';
      
CREATE ROLE IF NOT EXISTS latam_tasty_data_scientist
    COMMENT = 'data scientist para latam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_bi
    COMMENT = 'business intelligence para latam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_data_app
    COMMENT = 'data application developer para latam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_dev
    COMMENT = 'developer para latam tasty bytes';
    
-- Gerarquía de los role 
GRANT ROLE latam_tasty_admin TO ROLE sysadmin;
GRANT ROLE latam_tasty_data_engineer TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_data_scientist TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_bi TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_data_app TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_dev TO ROLE latam_tasty_data_engineer;



-- Permisos a capacidades  
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE latam_tasty_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE latam_tasty_admin;


-- Permiso a objetos 
USE ROLE securityadmin;

GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_admin;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_bi;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_app;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_bi;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_app;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_dev;

GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_admin;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_engineer;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_scientist;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_bi;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_app;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_dev;

GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;



-- Permisos a warehouse
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE sysadmin;
GRANT OWNERSHIP ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_admin REVOKE CURRENT GRANTS;

GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_admin;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_engineer;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_bi;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_scientist;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_app;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_dev;





-- Privilegios futuros  
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE FUNCTIONS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;

GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;




/********
Inicio de proceso de protección de datos 
********/


-- Aplicar etiquetado (Tag) para clasificaición de datos 
GRANT CREATE TAG ON SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_admin;
GRANT CREATE TAG ON SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_engineer;


-- Asociar etiquetado(tag) a futuras políticas de enmascaramiento 
USE ROLE accountadmin;
GRANT APPLY TAG ON ACCOUNT TO ROLE latam_tasty_admin;
GRANT APPLY TAG ON ACCOUNT TO ROLE latam_tasty_data_engineer;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE latam_tasty_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE latam_tasty_data_engineer;




/******
Inicio de conexión a fuente de datos externa (AWS S3) que cotiene archivos en formato CSV
******/



  
-- Colocación de datos en tabla para esquema raw_pos 
USE ROLE sysadmin;
USE WAREHOUSE latam_build_wh;


--Creación de tipo de formato archivo (file format)  CSV
CREATE OR REPLACE FILE FORMAT latam_frostbyte_tasty_bytes.public.csv_ff 
type = 'csv';



--Creación de stage (AWS S3) que contiene los datos origen
CREATE OR REPLACE STAGE latam_frostbyte_tasty_bytes.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = latam_frostbyte_tasty_bytes.public.csv_ff;


list @latam_frostbyte_tasty_bytes.public.s3load;

-- Creación de modelo  de datos (tabla) para datos en crudo (raw zone)


-- Crear tabla pais (country) 
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- Crear tabla franquicias (franchise)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

-- Crear tabla hubicación (location)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- Crear table menú (productos)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- Crear tabla punto de venta (truck)   
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- Crear table de pedidios (order_header)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);


-- Crear table de detalle de pedidios (order_detail)  
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- Crear table lealtad de cliente (customer loyalty)  
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);





/*
Consolidación de vistas armonizada (harmonized view)
*/



-- Creación de vistas: Pedidos (orders_v) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM latam_frostbyte_tasty_bytes.raw_pos.order_detail od
JOIN latam_frostbyte_tasty_bytes.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;




    
-- Creación de vista: métricas de lealtad (loyalty_metrics_v) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
JOIN latam_frostbyte_tasty_bytes.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;






/*
 Creación de vistas para Analitica  
*/

-- Creación de vista: Pedidios (orders_v) para esquema de Analítica (analytics)
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o;


-- Creación de vista: metricas de lealtad cliente (customer_loyalty_metrics_v)  para esquema analítica (analytics) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM latam_frostbyte_tasty_bytes.harmonized.customer_loyalty_metrics_v;






/*****
 Carga de datos desde integración stage fuente extarna (AWS S3) hacia Snowflake  
******/


alter warehouse latam_build_wh set WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Carga de datos a la tabla country 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.country
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/country/;

-- Carga de datos a la tabla franquicias (franchise) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.franchise
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.location
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/location/;

-- Carga de datos a la tabla (menu) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.menu
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/menu/;

-- Carga de datos a la tabla puntos de venta (truck) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.truck
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/truck/;

-- Carga de datos a la tabla lealtad de clientes (customer_loyalty) 
COPY INTO latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_customer/customer_loyalty/;

-- Carga de datos a la tabla pedidos (order_header) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.order_header
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/order_header/;

-- Carga de datos a la tabla detalle de pedidos (order_detail) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.order_detail
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/order_detail/;


-- Reducir capacidad wahrehouse  latam_build_wh
alter warehouse latam_build_wh set WAREHOUSE_SIZE = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- setup completado 
SELECT 'Intalación de ambiente tastybytes completado' AS note;