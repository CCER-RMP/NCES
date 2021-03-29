
{{ config(materialized='view') }}

SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,TEACHERS
FROM {{ source('source_data', 'staff2020') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,TEACHERS
FROM {{ source('source_data', 'staff2019') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,TEACHERS
FROM {{ source('source_data', 'staff2018') }}
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,TEACHERS
FROM {{ source('source_data', 'staff2017') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,FTE
FROM {{ source('source_data', 'staff2016') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,FTE
FROM {{ source('source_data', 'staff2015') }}
