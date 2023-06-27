/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Transformación
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/



-- Section 3: Step 1 - Create a Clone of Production
USE ROLE latam_tasty_dev;

CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
    CLONE latam_frostbyte_tasty_bytes.raw_pos.truck;

      
/*----------------------------------------------------------------------------------
Quickstart Sección 4: Probar la memoria caché del conjunto de resultados de consultas de Snowflakes

  Con nuestro Zero Copy Clone, disponible al instante, ahora podemos comenzar a desarrollar contra
  sin temor a afectar la producción. Sin embargo, antes de hacer cualquier cambio
  primero ejecutemos algunas consultas simples contra él y probemos Snowflakes
  Caché del conjunto de resultados.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1 - Consultando nuestra Tabla Clonada
USE WAREHOUSE latam_build_wh;

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;


-- Sección 4: Paso 2 - Volver a ejecutar nuestra consulta
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;

    
/*----------------------------------------------------------------------------------
Quickstart Sección 5: Actualización de datos y cálculo de antigüedad de los camiones de comida

  Según nuestro resultado anterior, primero debemos abordar el error tipográfico en esos registros Ford_
  vimos en nuestra columna `make`. A partir de ahí, podemos empezar a trabajar en nuestro cálculo.
  que nos dará la edad de cada camión.
-------------------------------------------------- ---------------------------------*/

-- Sección 5: Paso 1 - Actualización de valores incorrectos en una columna
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
SET make = 'Ford' 
WHERE make = 'Ford_';


-- Sección 5: Paso 2 - Elaboración de un cálculo de edad
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model,
    (YEAR(CURRENT_DATE()) - t.year) AS truck_age_year
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Quickstart Sección 6: Agregar una columna y actualizarla

  Con nuestro cálculo de edad de camiones en años hecho y desempolvado, ahora agreguemos una nueva columna
  a nuestra tabla clonada para respaldarla y terminar las cosas actualizando la columna a
  reflejan los valores calculados.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1 - Adición de una columna a una tabla
ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev
    ADD COLUMN truck_age NUMBER(4);


-- Sección 6: Paso 2 - Agregar valores calculados a nuestra columna
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
    SET truck_age = (YEAR(CURRENT_DATE()) / t.year);


-- Sección 6: Paso 3 - Consultando nuestra nueva Columna
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Quickstart Sección 7: Utilización del viaje en el tiempo para la recuperación de datos ante desastres

  Aunque cometimos un error, Snowflake tiene muchas funciones que pueden ayudarnos a salir
  de problemas aquí. El proceso que tomaremos aprovechará el historial de consultas, las variables SQL
  y Time Travel para revertir nuestra tabla `truck_dev` a lo que parecía antes
  a esa declaración de actualización incorrecta.
-------------------------------------------------- ---------------------------------*/

-- Sección 7: Paso 1: aprovechar el historial de consultas
SELECT 
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.query_history())
WHERE 1=1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%latam_frostbyte_tasty_bytes.raw_pos.truck_dev%'
ORDER BY start_time DESC;


-- Sección 7: Paso 2 - Configuración de una variable de entorno SQL
SET query_id = 
(
    SELECT TOP 1 query_id
    FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.query_history())
    WHERE 1=1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_age = (YEAR(CURRENT_DATE()) / t.year);'
    ORDER BY start_time DESC
);

-- verificar el Query ID
select $query_id;



-- Sección 7: Paso 3 - Aprovechar el Time-Travel para revertir nuestra tabla
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev
    AS 
SELECT * FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev
BEFORE(STATEMENT => $query_id); 


--Tabla restaurada antes del UPDATE sin columna truck_age
SELECT * FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;


/*----------------------------------------------------------------------------------
Quickstart Sección 8: Utilización del Time-Travel para la recuperación de datos ante desastres

  Aunque cometimos un error, Snowflake tiene muchas funciones que pueden ayudarnos a salir
  de problemas aquí. El proceso que tomaremos aprovechará el historial de consultas, las variables SQL
  y Time Travel para revertir nuestra tabla `truck_dev` a lo que parecía antes
  a esa declaración de actualización incorrecta.
-------------------------------------------------- ---------------------------------*/


-- Sección 8: Paso 1 - Adición de valores calculados correctamente a nuestra columna
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
SET truck_age = (YEAR(CURRENT_DATE()) - t.year);


-- Sección 8: Paso 2 - Intercambio de nuestra tabla de desarrollo con producción
USE ROLE sysadmin;

ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
    SWAP WITH latam_frostbyte_tasty_bytes.raw_pos.truck;


-- Sección 8: Paso 3 - Validar producción
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM latam_frostbyte_tasty_bytes.raw_pos.truck t
WHERE t.make = 'Ford';


/*----------------------------------------------------------------------------------
Quickstart Sección 9: Mesas de Descarte y Descarte

  Podemos decir oficialmente que nuestro desarrollador completó con éxito la tarea asignada.
  Con la columna truck_age en su lugar y calculada correctamente, nuestro administrador de sistemas puede
  limpia las mesas sobrantes para terminar las cosas.
-------------------------------------------------- ---------------------------------*/

-- Section 9: Step 1 - Eliminar Tabla
DROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck;

-- Intentar ver tabla Turck
select * from latam_frostbyte_tasty_bytes.raw_pos.truck;


-- Section 9: Step 2 - Restaurar Table
UNDROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck;

-- Intentar ver tabla Turck
select * from latam_frostbyte_tasty_bytes.raw_pos.truck;

-- Section 9: Step 3 - Eliminar Tabla
DROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/
USE ROLE accountadmin;
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck SET make = 'Ford_' WHERE make = 'Ford';
ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck DROP COLUMN truck_age;
UNSET query_id;
 