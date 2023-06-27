/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Monitor de Recursos
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/




/*----------------------------------------------------------------------------------
Quickstart Sección 3 - Creación de un almacén

  Como administrador de Tasty Bytes Snowflake, se nos ha encomendado la tarea de obtener un
  comprensión de las funciones que proporciona Snowflake para ayudar a garantizar
  La gobernanza financiera está lista antes de que comencemos a consultar y analizar datos.
 
  Comencemos creando nuestro primer almacén.
-------------------------------------------------- ---------------------------------*/

-- Sección 3: Paso 1 - Contexto de rol y Warehouse 
USE ROLE latam_tasty_admin;



-- Sección 3: Paso 2 - Creación y configuración de un Warehouse
CREATE OR REPLACE WAREHOUSE tasty_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall' 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 2 
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true
    INITIALLY_SUSPENDED = true;
    

/*----------------------------------------------------------------------------------
Quickstart Section 4 - Crear un Monitor de Recursos y Aplicarlo a nuestro Almacén

  Con un almacén en su lugar, ahora aprovechemos los monitores de recursos de Snowflakes para garantizar
  el almacén tiene una cuota mensual que permitirá a nuestros administradores realizar un seguimiento de su
  créditos consumidos y asegurarse de que se suspenda si excede su cuota asignada.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1: creación de un monitor de recursos
USE ROLE accountadmin;
CREATE OR REPLACE RESOURCE MONITOR latam_tasty_test_rm
WITH 
    CREDIT_QUOTA = 100 -- 100 créditos
    FREQUENCY = monthly -- reiniciar el monitor mensualmente
    START_TIMESTAMP = immediately -- comenzar a rastrear inmediatamente
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY -- notificar a los administradores de cuentas al 75%
        ON 100 PERCENT DO SUSPEND -- suspender el almacén al 100 por ciento, dejar que finalicen las consultas
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspender almacén y cancelar todas las consultas al 110%


-- Sección 4: Paso 2 - Aplicación de nuestro Monitor de recursos a nuestro Warehouse
ALTER WAREHOUSE tasty_test_wh SET RESOURCE_MONITOR = latam_tasty_test_rm;


/*----------------------------------------------------------------------------------
Quickstart Sección 5 - Protección de nuestro almacén de consultas de larga duración

  Con el monitoreo implementado, ahora asegurémonos de protegernos de los malos,
  Consultas de ejecución prolongada que garantizan que los parámetros de tiempo de espera se ajusten en el almacén.
-------------------------------------------------- ---------------------------------*/

-- Sección 5: Paso 1 - Exploración de los parámetros de declaración de Warehouse
SHOW PARAMETERS LIKE '%statement%' IN WAREHOUSE tasty_test_wh;


-- Sección 5: Paso 2 - Ajuste del parámetro de tiempo de espera de Whareouse
ALTER WAREHOUSE tasty_test_wh SET statement_timeout_in_seconds = 1800;


-- Sección 5: Paso 3 - Ajuste del parámetro de tiempo de espera en cola  de Warehouse
ALTER WAREHOUSE tasty_test_wh SET statement_queued_timeout_in_seconds = 600;


/*----------------------------------------------------------------------------------
Quickstart Sección 6 - Protección de nuestra cuenta de consultas de larga duración

  Estos parámetros de tiempo de espera también están disponibles a nivel de cuenta, usuario y sesión.
  Como no esperamos consultas extremadamente largas, también ajustemos estas
  parámetros en nuestra cuenta.
 
  En el futuro, planearemos monitorear estos como nuestras cargas de trabajo y uso de Snowflake
  crecer para garantizar que continúen protegiendo nuestra cuenta del consumo innecesario
  pero tampoco cancelar trabajos más largos que esperamos ejecutar.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1 - Ajuste del parámetro de tiempo de espera del estado de cuenta
ALTER ACCOUNT SET statement_timeout_in_seconds = 18000; 


-- Sección 6: Paso 2 - Ajuste del parámetro de tiempo de espera en espera del estado de cuenta
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = 3600; 


/*----------------------------------------------------------------------------------
Quickstart Section 7 -Aprovechando, Escalando y Suspendiendo nuestro Almacén

  Con los componentes básicos de la gobernanza financiera en su lugar, aprovechemos ahora el Snowflake
  Warehouse que creamos para ejecutar consultas. En el camino, escalamos este wahreouse
  hacia arriba y hacia abajo, así como probar la suspensión manual.
-------------------------------------------------- ---------------------------------*/

-- Sección 7: Paso 1: use nuestro Warehouse para ejecutar una consulta simple
USE ROLE latam_tasty_admin;
USE WAREHOUSE tasty_test_wh; 

    --> encontrar artículos de menú vendidos en Cheeky Greek
SELECT 
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name
FROM frostbyte_tasty_bytes.raw_pos.menu m
WHERE truck_brand_name = 'Cheeky Greek';


-- Sección 7: Paso 2 - Ampliar nuestro almacén
ALTER WAREHOUSE tasty_test_wh SET warehouse_size = 'XLarge';


-- Sección 7: Paso 3: ejecutar una consulta de agregación en un conjunto de datos grande
     --> calcular pedidos y ventas totales para nuestros miembros de fidelización de clientes
    
SELECT 
    o.customer_id,
    CONCAT(clm.first_name, ' ', clm.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM frostbyte_tasty_bytes.analytics.orders_v o
JOIN frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
    ON o.customer_id = clm.customer_id
GROUP BY o.customer_id, name
ORDER BY order_count DESC;


-- Sección 7: Paso 4 - Escalar nuestro Warehose hacia abajo
ALTER WAREHOUSE tasty_test_wh SET warehouse_size = 'XSmall';


-- Sección 7: Paso 5 - Suspender nuestro Warehouse
ALTER WAREHOUSE tasty_test_wh SUSPEND;
   
/*--
    "Estado no válido. El Warehose no se puede suspender". - AUTO_SUSPEND = 60 ya ocurrió
--*/



/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/

USE ROLE accountadmin;
ALTER ACCOUNT SET statement_timeout_in_seconds = default;
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = default; 
DROP WAREHOUSE IF EXISTS tasty_test_wh;
DROP RESOURCE MONITOR IF EXISTS latam_tasty_test_rm; 