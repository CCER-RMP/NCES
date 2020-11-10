
-- these files have fixed-length fields, and the starting positions/lengths of fields vary a lot
-- across years, hence this horribleness

WITH
schools_2003_to_2006 AS (
    SELECT
        '2002-2003' AS AcademicYear
        ,*
        ,RTRIM(SUBSTR(LINE, 1446, 5)) AS StudentTeacherRatio
    FROM {{ source('source_data', 'schools2003') }}
    UNION ALL
    SELECT
        '2003-2004' AS AcademicYear
        ,*
        ,RTRIM(SUBSTR(LINE, 1446, 5)) AS StudentTeacherRatio
    FROM {{ source('source_data', 'schools2004') }}
    UNION ALL
    SELECT
        '2004-2005' AS AcademicYear
        ,*
        ,RTRIM(SUBSTR(LINE, 1447, 6)) AS StudentTeacherRatio
    FROM {{ source('source_data', 'schools2005') }}
    UNION ALL
    SELECT
        '2005-2006' AS AcademicYear
        ,*
        ,RTRIM(SUBSTR(LINE, 1447, 5)) AS StudentTeacherRatio
    FROM {{ source('source_data', 'schools2006') }}
)
,Unioned as (
--        SELECT
--            LINE
--            ,'1999-2000' AS AcademicYear
--            ,RTRIM(SUBSTR(LINE, 317, 2)) AS LowGrade
--            ,RTRIM(SUBSTR(LINE, 319, 2)) AS HighGrade
--            -- TODO: County Name isn't a field in this year's file
--            ,NULL AS CountyName 
--            ,RTRIM(SUBSTR(LINE, 311, 1)) AS LocaleCode
--            ,CASE RTRIM(SUBSTR(LINE, 324, 1))
--                WHEN '1' THEN 'Yes'
--                WHEN '2' THEN 'No'
--                ELSE NULL
--            END AS Charter
--            ,CASE RTRIM(SUBSTR(LINE, 323, 1))
--                WHEN '1' THEN 'Yes'
--                WHEN '2' THEN 'No'
--                ELSE NULL
--            END AS Magnet
--            ,CASE RTRIM(SUBSTR(LINE, 321, 1))
--                WHEN '1' THEN 'Yes'
--                WHEN '2' THEN 'No'
--                ELSE NULL
--            END AS TitleISchool
--            ,CASE RTRIM(SUBSTR(LINE, 322, 1))
--                WHEN '1' THEN 'Yes'
--                WHEN '2' THEN 'No'
--                ELSE NULL
--            END AS TitleISchoolWide
--            ,RTRIM(SUBSTR(LINE, 1301, 4)) AS Students
--            ,RTRIM(SUBSTR(LINE, 312, 5)) AS Teachers
--            ,RTRIM(SUBSTR(LINE, 1389, 5)) AS StudentTeacherRatio
--            ,RTRIM(SUBSTR(LINE, 325, 4)) AS FreeLunch
--            ,RTRIM(SUBSTR(LINE, 329, 4)) AS ReducedLunch
--            -- no lat/lng in 2000 and prior
--            ,NULL AS Latitude
--            ,NULL AS Longitude
--        FROM schools2000
--        UNION ALL
    SELECT
        LINE
        ,'2000-2001' AS AcademicYear
        ,RTRIM(SUBSTR(LINE, 337, 2)) AS LowGrade
        ,RTRIM(SUBSTR(LINE, 339, 2)) AS HighGrade
        -- TODO: County Name isn't a field in this year's file
        ,NULL AS CountyName 
        ,RTRIM(SUBSTR(LINE, 311, 1)) AS LocaleCode
        ,CASE RTRIM(SUBSTR(LINE, 345, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Charter
        ,CASE RTRIM(SUBSTR(LINE, 344, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Magnet
        ,CASE RTRIM(SUBSTR(LINE, 342, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchool
        ,CASE RTRIM(SUBSTR(LINE, 343, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchoolWide
        ,RTRIM(SUBSTR(LINE, 1322, 4)) AS Students
        ,RTRIM(SUBSTR(LINE, 332, 5)) AS Teachers
        ,RTRIM(SUBSTR(LINE, 1410, 5)) AS StudentTeacherRatio
        ,RTRIM(SUBSTR(LINE, 346, 4)) AS FreeLunch
        ,RTRIM(SUBSTR(LINE, 350, 4)) AS ReducedLunch
        ,TRIM(SUBSTR(LINE, 312, 10)) AS Latitude
        ,TRIM(SUBSTR(LINE, 322, 10)) AS Longitude
    FROM {{ source('source_data', 'schools2001') }}
    UNION ALL
    SELECT
        LINE
        ,'2001-2002' AS AcademicYear
        ,RTRIM(SUBSTR(LINE, 338, 2)) AS LowGrade
        ,RTRIM(SUBSTR(LINE, 340, 2)) AS HighGrade
        -- TODO: County Name isn't a field in this year's file
        ,NULL AS CountyName 
        ,RTRIM(SUBSTR(LINE, 311, 1)) AS LocaleCode
        ,CASE RTRIM(SUBSTR(LINE, 346, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Charter
        ,CASE RTRIM(SUBSTR(LINE, 345, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Magnet
        ,CASE RTRIM(SUBSTR(LINE, 343, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchool
        ,CASE RTRIM(SUBSTR(LINE, 344, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchoolWide
        ,RTRIM(SUBSTR(LINE, 1323, 4)) AS Students
        ,RTRIM(SUBSTR(LINE, 333, 5)) AS Teachers
        ,RTRIM(SUBSTR(LINE, 1411, 5)) AS StudentTeacherRatio
        ,RTRIM(SUBSTR(LINE, 347, 4)) AS FreeLunch
        ,RTRIM(SUBSTR(LINE, 351, 4)) AS ReducedLunch
        ,TRIM(SUBSTR(LINE, 313, 10)) AS Latitude
        ,TRIM(SUBSTR(LINE, 323, 10)) AS Longitude
    FROM {{ source('source_data', 'schools2002') }}
    UNION ALL
    SELECT
        LINE
        ,AcademicYear
        ,RTRIM(SUBSTR(LINE, 373, 2)) AS LowGrade
        ,RTRIM(SUBSTR(LINE, 375, 2)) AS HighGrade
        ,RTRIM(SUBSTR(LINE, 338, 30)) AS CountyName
        ,RTRIM(SUBSTR(LINE, 311, 1)) AS LocaleCode
        ,CASE RTRIM(SUBSTR(LINE, 381, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Charter
        ,CASE RTRIM(SUBSTR(LINE, 380, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Magnet
        ,CASE RTRIM(SUBSTR(LINE, 378, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchool
        ,CASE RTRIM(SUBSTR(LINE, 379, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchoolWide
        ,RTRIM(SUBSTR(LINE, 1359, 4)) AS Students
        ,RTRIM(SUBSTR(LINE, 368, 5)) AS Teachers
        ,StudentTeacherRatio
        ,RTRIM(SUBSTR(LINE, 383, 4)) AS FreeLunch
        ,RTRIM(SUBSTR(LINE, 387, 4)) AS ReducedLunch
        ,TRIM(SUBSTR(LINE, 313, 10)) AS Latitude
        ,TRIM(SUBSTR(LINE, 323, 10)) AS Longitude
    FROM schools_2003_to_2006
    UNION ALL
    SELECT
        LINE
        ,'2006-2007' AS AcademicYear
        ,RTRIM(SUBSTR(LINE, 377, 2)) AS LowGrade
        ,RTRIM(SUBSTR(LINE, 379, 2)) AS HighGrade
        ,RTRIM(SUBSTR(LINE, 342, 30)) AS CountyName
        ,RTRIM(SUBSTR(LINE, 311, 2)) AS LocaleCode
        ,CASE RTRIM(SUBSTR(LINE, 385, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Charter
        ,CASE RTRIM(SUBSTR(LINE, 384, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS Magnet
        ,CASE RTRIM(SUBSTR(LINE, 382, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchool
        ,CASE RTRIM(SUBSTR(LINE, 383, 1))
            WHEN '1' THEN 'Yes'
            WHEN '2' THEN 'No'
            ELSE NULL
        END AS TitleISchoolWide
        ,RTRIM(SUBSTR(LINE, 1363, 4)) AS Students
        ,RTRIM(SUBSTR(LINE, 372, 5)) AS Teachers
        ,RTRIM(SUBSTR(LINE, 1451, 6)) AS StudentTeacherRatio
        ,RTRIM(SUBSTR(LINE, 387, 4)) AS FreeLunch
        ,RTRIM(SUBSTR(LINE, 391, 4)) AS ReducedLunch
        ,RTRIM(SUBSTR(LINE, 313, 9)) AS Latitude
        ,RTRIM(SUBSTR(LINE, 322, 11)) AS Longitude
    FROM {{ source('source_data', 'schools2007') }}
)
,Named AS (
    SELECT
        AcademicYear
        ,RTRIM(SUBSTR(LINE, 1, 12)) AS NCESSchoolID
        ,RTRIM(SUBSTR(LINE, 27, 20)) AS StateSchoolID
        ,RTRIM(SUBSTR(LINE, 1, 7)) AS NCESDistrictID
        ,RTRIM(SUBSTR(LINE, 13, 14)) AS StateDistrictID
        ,LowGrade
        ,HighGrade
        ,RTRIM(SUBSTR(LINE, 107, 50)) AS SchoolName
        ,RTRIM(SUBSTR(LINE, 47, 60)) AS District
        ,CountyName
        ,RTRIM(SUBSTR(LINE, 238, 30)) AS StreetAddress
        ,RTRIM(SUBSTR(LINE, 268, 30)) AS City
        ,RTRIM(SUBSTR(LINE, 298, 2)) AS State
        ,RTRIM(SUBSTR(LINE, 300, 5)) AS ZIP
        ,RTRIM(SUBSTR(LINE, 305, 4)) AS ZIP4
        ,RTRIM(SUBSTR(LINE, 157, 10)) AS Phone
        ,LocaleCode
        ,Charter
        ,Magnet
        ,TitleISchool
        ,TitleISchoolWide
        ,CASE WHEN CAST(Students AS FLOAT) >= 0 THEN Students ELSE NULL END AS Students
        ,CASE WHEN CAST(Teachers AS FLOAT) >= 0 THEN Teachers ELSE NULL END AS Teachers
        ,CASE WHEN CAST(StudentTeacherRatio AS FLOAT) >= 0 THEN StudentTeacherRatio ELSE NULL END AS StudentTeacherRatio
        ,CASE WHEN CAST(FreeLunch AS FLOAT) >= 0 THEN FreeLunch ELSE NULL END AS FreeLunch
        ,CASE WHEN CAST(ReducedLunch AS FLOAT) >= 0 THEN ReducedLunch ELSE NULL END AS ReducedLunch
        ,CAST(CASE
            WHEN Latitude <> 'N' THEN
                CASE
                    WHEN Latitude NOT LIKE '%.%' THEN
                        CASE
                            WHEN Latitude LIKE '%-%' THEN SUBSTR(Latitude, 1, 4) || '.' || SUBSTR(Latitude, 5)
                            ELSE SUBSTR(Latitude, 1, 3) || '.' || SUBSTR(Latitude, 4)
                        END
                    ELSE Latitude
                END
            ELSE NULL
        END AS Float) AS Latitude
        ,CAST(CASE
            WHEN Longitude <> 'N' THEN
                CASE
                    WHEN Longitude NOT LIKE '%.%' THEN
                        CASE
                            WHEN Longitude LIKE '%-%' THEN SUBSTR(Longitude, 1, 4) || '.' || SUBSTR(Longitude, 5)
                            ELSE SUBSTR(Longitude, 1, 3) || '.' || SUBSTR(Longitude, 4)
                        END
                    ELSE Longitude
                END
            ELSE NULL
        END AS Float) AS Longitude
    FROM Unioned
)
SELECT
    AcademicYear
    ,NCESSchoolID
    ,State || '-' || StateDistrictID || '-' || StateSchoolID AS StateSchoolID
    ,NCESDistrictID
    ,State || '-' || StateDistrictID AS StateDistrictID
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
    ,LocaleCode
    ,Charter
    ,Magnet
    ,TitleISchool
    ,TitleISchoolWide
    ,Students
    ,Teachers
    ,StudentTeacherRatio
    ,FreeLunch
    ,ReducedLunch
    ,Latitude
    ,Longitude
FROM Named
