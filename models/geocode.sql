
{{ config(materialized='view') }}

SELECT
 SCHOOLYEAR
 ,NCESSCH
 ,NMCNTY
 ,LOCALE
 ,LAT
 ,LON
FROM {{ source('source_data', 'geocodeRaw2020') }}
UNION ALL
SELECT
 SCHOOLYEAR
 ,NCESSCH
 ,NMCNTY
 ,LOCALE
 ,LAT
 ,LON
FROM {{ source('source_data', 'geocodeRaw2019') }}
UNION ALL
SELECT
 SCHOOLYEAR
 ,NCESSCH
 ,NMCNTY
 ,LOCALE
 ,LAT
 ,LON
FROM {{ source('source_data', 'geocodeRaw2018') }}
UNION ALL
SELECT
 SCHOOLYEAR || '-' || (CAST(SCHOOLYEAR AS INT) + 1) AS SCHOOLYEAR
 ,NCESSCH
 ,NMCNTY
 ,LOCALE
 ,LAT
 ,LON
FROM {{ source('source_data', 'geocodeRaw2017') }}
UNION ALL
SELECT
 '2015-2016' AS SCHOOLYEAR
 ,NCESSCH
 ,NMCNTY15
 ,LOCALE15
 ,LAT1516
 ,LON1516
FROM {{ source('source_data', 'geocodeRaw2016') }}
UNION ALL
SELECT
 SURVYEAR || '-' || (CAST(SURVYEAR AS INT) + 1) AS SCHOOLYEAR
 ,NCESSCH
 ,CONAME
 ,LOCALE
 ,LATCODE
 ,LONGCODE
FROM {{ source('source_data', 'geocodeRaw2015') }}
