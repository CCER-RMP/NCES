
{{ config(materialized='view') }}

SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'membership2020') }}
WHERE TOTAL_INDICATOR = 'Derived - Education Unit Total minus Adult Education Count'
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'membership2019') }}
WHERE TOTAL_INDICATOR = 'Derived - Education Unit Total minus Adult Education Count'
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'membership2018') }}
WHERE TOTAL_INDICATOR = 'Derived - Education Unit Total minus Adult Education Count'
UNION ALL
SELECT
    SCHOOL_YEAR
    ,NCESSCH
    ,STUDENT_COUNT
FROM {{ source('source_data', 'membership2017') }}
WHERE TOTAL_INDICATOR = 'Derived - Education Unit Total minus Adult Education Count'
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,MEMBER AS STUDENT_COUNT
FROM {{ source('source_data', 'membership2016') }}
UNION ALL
SELECT
    SURVYEAR AS SCHOOL_YEAR
    ,NCESSCH
    ,MEMBER AS STUDENT_COUNT
FROM {{ source('source_data', 'membership2015') }}
