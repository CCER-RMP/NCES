
{{ config(materialized='table') }}

WITH T AS (
    SELECT
    *
    FROM {{ ref('schools_2007_and_prior') }}

    UNION ALL

    SELECT
    *
    FROM {{ ref('schools_2008_2014') }}

    UNION ALL

    SELECT
    *
    FROM {{ ref('schools_2015_onwards') }}
)
SELECT
    AcademicYear
    ,NCESSchoolID
    ,StateSchoolID
    ,NCESDistrictID
    ,StateDistrictID
    ,LowGrade
    ,HighGrade
    ,SchoolName
    ,District
    ,CountyName
    ,StreetAddress
    ,City
    ,State
    ,ZIP
    ,ZIP4
    ,Phone
    -- null out Missing and N/A
    ,CASE WHEN T.LocaleCode NOT IN ('M', 'N') THEN T.LocaleCode ELSE NULL END AS LocaleCode
    ,lc.Locale
    ,Charter
    ,Magnet
    ,TitleISchool
    ,TitleISchoolWide
    -- cast to remove leading 0's in early years
    ,CAST(Students AS INT) as Students
    ,CAST(Teachers as FLOAT) as Teachers
    ,CAST(StudentTeacherRatio as float) as StudentTeacherRatio
    ,CAST(FreeLunch AS INT) As FreeLunch
    ,CAST(ReducedLunch AS INT) AS ReducedLunch
    ,CASE WHEN CAST(Latitude AS Float) <> 0.0 THEN Latitude ELSE NULL END as Latitude
    ,CASE WHEN CAST(Longitude AS Float) <> 0.0 THEN Longitude ELSE NULL END as Longitude
FROM T
LEFT JOIN {{ ref('locale_codes') }}  lc
    ON T.LocaleCode = lc.LocaleCode
