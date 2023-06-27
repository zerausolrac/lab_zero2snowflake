/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Gobierno de Datos
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/

-- Sección 3: Paso 1 - Establecer nuestro contexto
USE ROLE accountadmin;

USE WAREHOUSE latam_build_wh;

alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Sección 3: Paso 2 - Exploración de todos los roles en nuestra cuenta
SHOW ROLES;


-- Sección 3: Paso 3 - Uso del análisis de resultados para filtrar nuestro resultado
SELECT 
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');


/*----------------------------------------------------------------------------------
Quickstart - Sección 3: Paso 3: uso del análisis de resultados para filtrar nuestro resultado Sección 4: creación de un rol y concesión de privilegios

  Ahora que entendemos estos roles definidos por el sistema, comencemos a aprovecharlos para
  cree un rol de prueba y concédale acceso a los datos de lealtad del cliente que implementaremos
  nuestras características iniciales de Data Governance contra y nuestro almacén tasty_dev_wh
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1: Usar el rol de Useradmin para crear nuestro rol de prueba
USE ROLE useradmin;

CREATE OR REPLACE ROLE latam_tasty_test_role
    COMMENT = 'test role for tasty bytes';

-- Sección 4: Paso 2: uso de la función Securityadmin para otorgar privilegios de almacén
USE ROLE securityadmin;
GRANT OPERATE, USAGE ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_test_role;


-- Sección 4: Paso 3: uso de la función Securityadmin para otorgar privilegios de base de datos y esquema
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_test_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_test_role;


-- Sección 4: Paso 4: uso de la función de administrador de seguridad para otorgar privilegios de visualización y tabla
GRANT SELECT ON ALL TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_test_role;
GRANT SELECT ON ALL TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_test_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_test_role;


-- Sección 4: Paso 5 - Uso de la función Securityadmin para nuestra función para nuestro usuario
SET my_user_var  = CURRENT_USER();
GRANT ROLE latam_tasty_test_role TO USER identifier($my_user_var);


/*----------------------------------------------------------------------------------
Quickstart Sección 4 - Crear y adjuntar etiquetas (Tags) a nuestras columnas PII

  El primer conjunto de características de Data Governance que queremos implementar y probar será Snowflake
  Enmascaramiento de datos dinámico basado en etiquetas. Esta característica nos permitirá enmascarar datos PII en
  columnas en el tiempo de ejecución de la consulta desde nuestro rol de prueba, pero déjelo expuesto a más
  roles privilegiados.

  Antes de que podamos comenzar a enmascarar datos, primero exploremos qué PII existe en nuestro
  Datos de fidelización de clientes.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1 - Encontrar nuestras columnas PII
USE ROLE latam_tasty_test_role;
USE WAREHOUSE latam_build_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl;


-- Sección 4: Paso 2 - Creación de etiquetas (Tags)
USE ROLE accountadmin;

CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag
    COMMENT = 'PII Tag for Name Columns';
    
CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    COMMENT = 'PII Tag for Phone Number Columns';
    
CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag
    COMMENT = 'PII Tag for E-mail Columns';


-- Sección 4 - Paso 3 - Aplicación de etiquetas (Tags)
ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN first_name 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'First Name';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN last_name 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'Last Name';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN phone_number 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag = 'Phone Number';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN e_mail
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag = 'E-mail Address';


-- Sección 4: Paso 4 - Exploración de etiquetas (Tags) en una tabla
SELECT 
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value 
FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.tag_references_all_columns
    ('latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty','table'));


/*----------------------------------------------------------------------------------
Quickstart Sección 5: creación y aplicación de políticas de enmascaramiento basadas en etiquetas

  Con nuestra base de etiquetas en su lugar, ahora podemos comenzar a desarrollar Dynamic Masking
  Políticas para admitir diferentes requisitos de enmascaramiento para nuestro nombre, número de teléfono
  y columnas de correo electrónico.
-------------------------------------------------- ---------------------------------*/

-- Sección 5: Paso 1 - Creación de políticas de enmascaramiento
USE ROLE sysadmin;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE '**~MASKED~**'
END;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT(LEFT(val,3), '-***-****')
END;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
END;
            

-- Sección 5: Paso 2 - Aplicación de etiquetas (Tags) de políticas de enmascaramiento 
USE ROLE accountadmin;

ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag 
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask;
    
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask;
    
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask;


/*----------------------------------------------------------------------------------
Quickstart Sección 6: Prueba de nuestras políticas de enmascaramiento basadas en etiquetas

  Con la implementación de nuestras Políticas de enmascaramiento basadas en etiquetas, validemos lo que
  hemos llevado a cabo hasta ahora para confirmar que logramos reunirnos con el cliente de Tasty Bytes
  Requisitos de enmascaramiento de datos de PII de fidelidad.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1: probar nuestra política de enmascaramiento en un rol que no sea de administrador
USE ROLE latam_tasty_test_role;
USE WAREHOUSE latam_build_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.city,
    cl.country
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- Sección 6: Paso 2: Probar nuestra política de enmascaramiento 
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


-- Sección 6: Paso 3: probar nuestra política de enmascaramiento en un rol de administrador
USE ROLE accountadmin;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE 1=1
    AND clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


/*----------------------------------------------------------------------------------
Quickstart Sección 7 - Implementación y prueba de seguridad a nivel de fila

Contento con nuestro enmascaramiento dinámico basado en etiquetas que controla el enmascaramiento a nivel de columna,
ahora buscaremos restringir el acceso en el nivel de fila para nuestro rol de prueba.

Dentro de nuestra tabla de lealtad del cliente, nuestro rol solo debe ver a los clientes que son
con sede en Tokio. Afortunadamente, Snowflake tiene otro poderoso Data Governance nativo
característica que puede manejar esto a escala llamada Políticas de acceso a filas.

Para nuestro caso de uso, aprovecharemos el enfoque de la tabla de mapeo.
-------------------------------------------------- ---------------------------------*/

-- Sección 7: Paso 1 - Creación de una tabla de mapeo
USE ROLE sysadmin;

CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.public.row_policy_map
    (role STRING, city_permissions STRING);

    
-- Sección 7: Paso 2 - Inserción de registros de mapeo
INSERT INTO latam_frostbyte_tasty_bytes.public.row_policy_map
    VALUES ('LATAM_TASTY_TEST_ROLE','Tokyo');


select * from latam_frostbyte_tasty_bytes.public.row_policy_map;    

-- Sección 7: Paso 3: creación de una política de acceso a filas
CREATE OR REPLACE ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy
       AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN -- lista de roles que no estarán sujetos a la política  
           (
            'ACCOUNTADMIN','SYSADMIN'
           )
        OR EXISTS -- esta cláusula hace referencia a nuestra tabla de mapeo desde arriba para manejar el filtrado de nivel de fila
            (
            SELECT rp.role 
                FROM latam_frostbyte_tasty_bytes.public.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            );

            
-- Sección 7: Paso 4: Aplicar una política de acceso a filas a una tabla
ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy ON (city);

    
-- Sección 7: Paso 5: probar nuestra política de acceso a filas en un rol sin privilegios
USE ROLE latam_tasty_test_role;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;


-- Sección 7: Paso 6: Probar nuestra política de acceso a filas 
SELECT 
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;


-- Sección 7: Paso 7: probar nuestra política de acceso a filas en un rol privilegiado
USE ROLE sysadmin;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;







/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/

USE ROLE accountadmin;

DROP ROLE IF EXISTS latam_tasty_test_role;

ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask;
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask;
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask;

DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag;
DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag;
DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag;

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
DROP ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy;

DROP TABLE IF EXISTS latam_frostbyte_tasty_bytes.public.row_policy_map;