/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Geoespacial
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/




/*----------------------------------------------------------------------------------
Quickstart Sección 3: Adquisición de datos de PDI de Safegraph del Snowflake Marketplace

  Tasty Bytes opera Food Trucks en numerosas ciudades y países de todo el
  mundo con cada camión que tiene la capacidad de elegir dos ubicaciones de venta diferentes
  por día. Un punto importante que interesa a nuestros Ejecutivos es aprender
  más sobre cómo estas ubicaciones se relacionan entre sí, así como si hay alguna
  ubicaciones a las que servimos actualmente que están potencialmente demasiado lejos de las más vendidas
  centros de la ciudad.

  Desafortunadamente, lo que hemos visto hasta ahora es que nuestros datos de primera mano no nos dan
  los componentes básicos necesarios para completar este tipo de análisis geoespacial.
 
  Afortunadamente, Snowflake Marketplace tiene excelentes listados de Safegraph que
  puede ayudarnos aquí.
-------------------------------------------------- ---------------------------------*/

-- Sección 3: Paso 1: uso de datos propios para encontrar las ubicaciones de mayor venta
USE ROLE latam_tasty_data_engineer;

USE WAREHOUSE latam_build_wh;

SELECT TOP 10
    o.location_id,
    SUM(o.price) AS total_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id
ORDER BY total_sales_usd DESC;


-- Section 3:Paso 2: Adquisición de datos de PDI de Safegraph del Snowflake Marketplace
/*--
     - Haga clic en -> Icono de inicio
     - Haga clic en -> marketplace
     - Buscar -> frostbyte
     - Haga clic en -> SafeGraph: frostbyte
     - Haga clic en -> Obtener
     - Cambiar el nombre de la base de datos -> FROSTBYTE_SAFEGRAPH (todas las letras mayúsculas)
     - Otorgar a roles adicionales -> PÚBLICO
--*/


-- Sección 3: Paso 3 - Evaluación de datos de PDI de Safegraph
SELECT 
    cpg.placekey,
    cpg.location_name,
    cpg.longitude,
    cpg.latitude,
    cpg.street_address,
    cpg.city,
    cpg.country,
    cpg.polygon_wkt
FROM frostbyte_safegraph.public.frostbyte_tb_safegraph_s cpg
WHERE 1=1
    AND cpg.top_category = 'Museums, Historical Sites, and Similar Institutions'
    AND cpg.sub_category = 'Museums'
    AND cpg.city = 'Paris'
    AND cpg.country = 'France';


/*----------------------------------------------------------------------------------
Quickstart Sectión 4 - Armonización y promoción de datos propios y de terceros

  Para que nuestro análisis geoespacial sea perfecto, asegurémonos de obtener Safegraph POI
  datos incluidos en analytics.orders_v para que todos nuestros usuarios intermedios puedan
  también acceder a ella.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1 - Enriquecer nuestra vista de análisis
USE ROLE sysadmin;

CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT 
    DATE(o.order_ts) AS date,
    o.* ,
    cpg.* EXCLUDE (location_id, region, phone_number, country)
FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o
JOIN frostbyte_safegraph.public.frostbyte_tb_safegraph_s cpg
    ON o.location_id = cpg.location_id;


/*----------------------------------------------------------------------------------
Quickstart Sección 5 - Realización de análisis geoespacial - Parte 1

  Con las métricas de puntos de interés ahora disponibles en Snowflake Marketplace
  sin necesidad de ETL, nuestro ingeniero de datos de Tasty Bytes ahora puede comenzar en nuestro
  Viaje de análisis geoespacial.
-------------------------------------------------- ---------------------------------*/    

-- Sección 5: Paso 1: creación de un punto geográfico
USE ROLE latam_tasty_data_engineer;

SELECT TOP 10 
    o.location_id,
    ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
    SUM(o.price) AS total_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id, o.latitude, o.longitude
ORDER BY total_sales_usd DESC;


-- Sección 5: Paso 2 - Cálculo de la distancia entre ubicaciones
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    a.location_id,
    b.location_id,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1609,2) AS geography_distance_miles,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1000,2) AS geography_distance_kilometers
FROM _top_10_locations a  
JOIN _top_10_locations b
    ON a.location_id <> b.location_id -- avoid calculating the distance between the point itself
QUALIFY a.location_id <> LAG(b.location_id) OVER (ORDER BY geography_distance_miles) -- avoid duplicate: a to b, b to a distances
ORDER BY geography_distance_miles;


/*----------------------------------------------------------------------------------
Quickstart Sección 6 - Realización de análisis geoespacial - Parte 1

  Ahora que entendemos cómo crear puntos y calcular la distancia, vamos a
  acumular un gran conjunto de funciones geoespaciales adicionales de Snowflake para promover nuestra
  análisis.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1 - Recopilación de puntos, dibujo de un polígono delimitador mínimo y cálculo del área
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    ST_NPOINTS(ST_COLLECT(tl.geo_point)) AS count_points_in_collection,
    ST_COLLECT(tl.geo_point) AS collection_of_points,
    ST_ENVELOPE(collection_of_points) AS minimum_bounding_polygon,
    ROUND(ST_AREA(minimum_bounding_polygon)/1000000,2) AS area_in_sq_kilometers
FROM _top_10_locations tl;


-- Sección 6: Paso 2: encontrar nuestras ubicaciones de mayor venta Punto central
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT  
    ST_COLLECT(tl.geo_point) AS collect_points,
    ST_CENTROID(collect_points) AS geometric_center_point
FROM _top_10_locations tl;


-- Sección 6: Paso 3 - Establecer una variable SQL como nuestro punto central
SET center_point = 'POINT(2.364853294993676e+00 4.885681511418426e+01)';


-- Sección 6: Paso 4: encontrar las ubicaciones más alejadas de nuestro punto central de mayor venta
WITH _2022_paris_locations AS
(
    SELECT DISTINCT 
        o.location_id,
        o.location_name,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
)
SELECT TOP 50
    ll.location_id,
    ll.location_name,
    ROUND(ST_DISTANCE(ll.geo_point, TO_GEOGRAPHY($center_point))/1000,2) AS kilometer_from_top_selling_center
FROM _2022_paris_locations ll
ORDER BY kilometer_from_top_selling_center DESC;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/

UNSET center_point;

USE ROLE sysdmin;

CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o;

DROP DATABASE IF EXISTS frostbyte_safegraph;