
{{ config(materialized='view') }}

SELECT 
    SCHOOL_YEAR
    ,NCESSCH
    ,ST_SCHID
    ,LEAID
    ,ST_LEAID
    ,GSLO
    ,GSHI
    ,SCH_NAME
    ,LEA_NAME
    ,LSTREET1
    ,LCITY
    ,LSTATE
    ,LZIP
    ,LZIP4
    ,PHONE
    ,CHARTER_TEXT
FROM {{ source('source_data', 'directory2019') }}
UNION ALL
SELECT 
    SCHOOL_YEAR
    ,NCESSCH
    ,ST_SCHID
    ,LEAID
    ,ST_LEAID
    ,GSLO
    ,GSHI
    ,SCH_NAME
    ,LEA_NAME
    ,LSTREET1
    ,LCITY
    ,LSTATE
    ,LZIP
    ,LZIP4
    ,PHONE
    ,CHARTER_TEXT
FROM {{ source('source_data', 'directory2018') }}
UNION ALL
SELECT 
    SCHOOL_YEAR
    ,NCESSCH
    ,ST_SCHID
    ,LEAID
    ,ST_LEAID
    ,GSLO
    ,GSHI
    ,SCH_NAME
    ,LEA_NAME
    ,LSTREET1
    ,LCITY
    ,LSTATE
    ,LZIP
    ,LZIP4
    ,PHONE
    ,CHARTER_TEXT
FROM {{ source('source_data', 'directory2017') }}
UNION ALL
SELECT 
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,ST_SCHID
    ,LEAID
    ,ST_LEAID
    ,GSLO
    ,GSHI
    ,SCH_NAME
    ,LEA_NAME
    ,LSTREET1
    ,LCITY
    ,LSTATE
    ,LZIP
    ,LZIP4
    ,PHONE
    ,CHARTER_TEXT
FROM {{ source('source_data', 'directory2016') }}
UNION ALL
SELECT 
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,ST_SCHID
    ,LEAID
    ,ST_LEAID
    ,GSLO
    ,GSHI
    ,SCH_NAME
    ,LEA_NAME
    ,LSTREET1
    ,LCITY
    ,LSTATE
    ,LZIP
    ,LZIP4
    ,PHONE
    ,CHARTER_TEXT
FROM {{ source('source_data', 'directory2015') }}
