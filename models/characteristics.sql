
{{ config(materialized='view') }}

SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2020') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2019') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2018') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2017') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2016') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,MAGNET_TEXT
    ,TITLEI_STATUS
FROM {{ source('source_data', 'characteristics2015') }}
