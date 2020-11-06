
{{ config(materialized='view') }}

SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'lunch2019') }}
WHERE TOTAL_INDICATOR = 'Category Set A' AND LUNCH_PROGRAM = 'Free lunch qualified'
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'lunch2018') }}
WHERE TOTAL_INDICATOR = 'Category Set A' AND LUNCH_PROGRAM = 'Free lunch qualified'
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'lunch2017') }}
WHERE TOTAL_INDICATOR = 'Category Set A' AND LUNCH_PROGRAM = 'Free lunch qualified'
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,FRELCH AS STUDENT_COUNT
FROM {{ source('source_data', 'lunch2016') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,FRELCH AS STUDENT_COUNT
FROM lunch2015
