/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam-  Zero to Snowflake - Dato Semi-Estructurado
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/


-- Sección 3: Paso 1 - Establecer nuestro contexto y consultar nuestra tabla
USE ROLE latam_tasty_data_engineer;

USE WAREHOUSE latam_build_wh;


alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- selecionar menú
SELECT TOP 10
    m.truck_brand_name,
    m.menu_type,
    m.menu_item_name,
    m.menu_item_health_metrics_obj
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m;


-- Sección 3: Paso 2 - Explorando nuestra columna semiestructurada
SHOW COLUMNS IN latam_frostbyte_tasty_bytes.raw_pos.menu;


-- Sección 3: Paso 3 - Travesía de datos semiestructurados usando la notación de puntos
SELECT 
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m;


/*----------------------------------------------------------------------------------
Quickstart Sección 4 - Aplanamiento de datos semiestructurados
  Habiendo visto cómo podemos consultar fácilmente los datos semiestructurados tal como existen en una variante
  columna utilizando la notación de puntos, nuestro Tasty Data Engineer está bien encaminado para proporcionar
  sus partes interesadas internas con los datos que han solicitado.

  Dentro de esta sección, llevaremos a cabo un procesamiento de datos semiestructurados adicional.
  para cumplir con los requisitos.
-------------------------------------------------- ---------------------------------*/

-- Sección 4: Paso 1 - Introducción a Lateral Flatten
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Sección 4: Paso 2 - Explorando una función de matriz
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, obj.value:"ingredients"::VARIANT);


-- Sección 4: Paso 3 - Estructuración de datos semiestructurados a escala
SELECT 
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;
    

/*----------------------------------------------------------------------------------
Quickstart Sección 5 - Creación de vistas estructuradas sobre datos semiestructurados

  En la última sección, construimos una consulta que proporciona el resultado exacto para nuestro fin
  los usuarios requieren el uso de un conjunto de funciones de datos semiestructurados de Snowflake junto con
  el camino. A continuación seguiremos el proceso de promoción de esta consulta contra nuestro Raw
  capa a través de Harmonized y eventualmente a Analytics donde nuestros usuarios finales están
  privilegiado para leer.
-------------------------------------------------- ---------------------------------*/

-- Sección 5: Paso 1 - Creando nuestra Vista Armonizada Usando nuestro SQL Aplanado Semi-Estructurado
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.menu_v
    AS
SELECT 
    m.menu_id,
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    m.item_category,
    m.item_subcategory,
    m.cost_of_goods_usd,
    m.sale_price_usd,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Sección 5: Paso 2: Promoción de Harmonized a Analytics con facilidad
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including cost, price, ingredients and dietary restrictions'
    AS
SELECT 
    * 
    EXCLUDE (menu_type_id) --exclude MENU_TYPE_ID
    RENAME  (truck_brand_name AS brand_name) -- rename TRUCK_BRAND_NAME to BRAND_NAME
FROM latam_frostbyte_tasty_bytes.harmonized.menu_v;


/*----------------------------------------------------------------------------------
Quickstart Sección 6: Análisis de datos semiestructurados procesados en Snowsight

  Con nuestra vista de menú disponible en nuestra capa de análisis, ejecutemos algunas consultas
  contra él que proporcionaremos a nuestros usuarios finales mostrando cómo Snowflake potencia
  una experiencia de consulta relacional sobre datos semiestructurados sin tener que hacer
  copias adicionales o realizar cualquier procesamiento complejo.
-------------------------------------------------- ---------------------------------*/

-- Sección 6: Paso 1 - Análisis de Arrays
SELECT 
    m1.menu_type,
    m1.menu_item_name,
    m2.menu_type AS overlap_menu_type,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m1
JOIN latam_frostbyte_tasty_bytes.analytics.menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- evitar unir el mismo elemento de menú a sí mismo
    AND m1.menu_type <> m2.menu_type 
WHERE 1=1
    AND m1.item_category <> 'Beverage' -- quitar bebidas
    AND m2.item_category <> 'Beverage' -- quitar bebidas
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- se evalúa como TRUE si un ingrediente está en ambas matrices
ORDER BY m1.menu_type;


-- Sección 6: Paso 2 - Brindar métricas a los ejecutivos
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_healthy_flag = 'Y' THEN 1 ELSE 0 END) AS healthy_item_count,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m;


-- Sección 6: Paso 3 - Conversión de resultados en gráficos
SELECT
    m.brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m
WHERE m.brand_name IN  ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.brand_name;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/*------ Se pueden ejecutar para restablecer su cuenta a un estado  ------*/
/*----- inicial, eso le permitirá volver a ejecutar este inicio rápido ---*/
/**************************************************** *********************/
DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.harmonized.menu_v;
DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.analytics.menu_v;