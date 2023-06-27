/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Colaboración
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/



-- Sección 3: Paso 1: consulta de datos de puntos de venta para tendencias
USE ROLE latam_tasty_data_engineer;
USE WAREHOUSE latam_build_wh;

alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


SELECT 
    o.date,
    SUM(o.price) AS daily_sales
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.country = 'Germany'
    AND o.primary_city = 'Hamburg'
    AND DATE(o.order_ts) BETWEEN '2022-02-10' AND '2022-02-28'
GROUP BY o.date
ORDER BY o.date ASC;


/*------------------------------------------------ ----------------------------------
QuickStart Sección 4: Investigación de los días de cero ventas en nuestros datos propios
  Por lo que vimos arriba, parece que nos faltan las ventas del 16 de febrero.
  hasta el 21 de febrero para Hamburgo. Dentro de nuestros datos propios no hay
  mucho más que podemos usar para investigar esto, pero algo más grande debe haber sido
  en juego aquí.
 
  Una idea que podemos explorar de inmediato aprovechando Snowflake Marketplace es
  clima extremo y una lista pública gratuita proporcionada por Weather Source.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1 - Adquisición de Weather Source LLC: Listado en el mercado de frostbyte Snowflake

/*---
     1. Haga clic en -> Icono de inicio
     2. Haga clic en -> Marketplace
     3. Buscar -> Frostbyte
     4. Haga clic en -> Weather Source LLC: frostbyte
     5. Haga clic en -> Obtener
     6. Cambiar el nombre de la base de datos -> FROSTBYTE_WEATHERSOURCE (todas las letras mayúsculas)
     7. Otorgar a roles adicionales -> PÚBLICO
---*/


-- Sección 4: Paso 2 - Armonización de datos propios y de terceros
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.daily_weather_v
    AS
SELECT 
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM frostbyte_weathersource.onpoint_id.history_day hd
JOIN frostbyte_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN latam_frostbyte_tasty_bytes.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;


-- Sección 4: Paso 3 - Visualización de temperaturas diarias
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS avg_temperature_air_2m_f
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


-- Sección 4: Paso 4 - Incorporación de métricas de viento y lluvia
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


/*----------------------------------------------------------------------------------
 Quickstart Sección 5: Democratización de las perspectivas de datos
 
   Ahora hemos determinado que los vientos de nivel de huracán probablemente estuvieron en juego para el
   días con cero ventas que nuestros analistas financieros nos señalaron.

   Ahora hagamos que este tipo de investigación esté disponible para cualquier miembro de nuestra organización.
   implementando una vista de Analytics a la que pueden acceder todos los empleados de Tasty Bytes.
-------------------------------------------------- ---------------------------------*/

-- Sección 5: Paso 1 - Creación de funciones SQL
     --> crear la función SQL que traduce Fahrenheit a Celsius
CREATE OR REPLACE FUNCTION latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
    (temp_f - 32) * (5/9)
$$;

    --> crear la función SQL que traduce pulgadas a milímetros
CREATE OR REPLACE FUNCTION latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;

-- Sección 5: Paso 2 - Creando el SQL para nuestra Vista
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN latam_frostbyte_tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;


-- Sección 5: Paso 3: implementación de nuestra vista de análisis
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Source Metrics and Orders Data for our Cities'
    AS
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN latam_frostbyte_tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;



/*----------------------------------------------------------------------------------
 Quickstart Sección 6: Obtención de conocimientos a partir de datos meteorológicos de ventas y del mercado
 
  Con datos de ventas y clima disponibles para todas las ciudades en las que operan nuestros camiones de comida,
  ahora echemos un vistazo al valor que hemos proporcionado a nuestros analistas financieros.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1 - Simplificando nuestro Análisis
SELECT 
    dcm.date,
    dcm.city_name,
    dcm.country_desc,
    dcm.daily_sales,
    dcm.avg_temperature_fahrenheit,
    dcm.avg_temperature_celsius,
    dcm.avg_precipitation_inches,
    dcm.avg_precipitation_millimeters,
    dcm.max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v dcm
WHERE 1=1
    AND dcm.country_desc = 'Germany'
    AND dcm.city_name = 'Hamburg'
    AND dcm.date BETWEEN '2022-02-01' AND '2022-02-24'
ORDER BY date DESC;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/

USE ROLE accountadmin;

DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.harmonized.daily_weather_v;
DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v;

DROP DATABASE IF EXISTS frostbyte_weathersource;

DROP FUNCTION IF EXISTS latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(NUMBER(35,4));
DROP FUNCTION IF EXISTS latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(NUMBER(35,4));