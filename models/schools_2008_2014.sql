
-- these years had variation in column names and years embedded in col names
-- teacher count field (FTE) is lways -1 or -2 for WA state schools

    WITH T AS (
        SELECT
            SURVYEAR || '-' || (CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE || '-' || STID || '-' || SEASCH AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE || '-' || STID AS StateDistrictID
            ,GSLO AS LowGrade
            ,GSHI AS HighGrade
            ,SCHNAM AS SchoolName
            ,LEANM AS District
            ,CONAME AS CountyName
            ,LSTREE AS StreetAddress
            ,LCITY as City
            ,LSTATE AS State
            ,LZIP AS ZIP
            ,LZIP4 AS ZIP4
            ,PHONE AS Phone
            ,ULOCAL AS LocaleCode
            ,CASE 
                WHEN CHARTR = '1' THEN 'Yes' 
                WHEN CHARTR = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET = '1' THEN 'Yes'
                WHEN MAGNET = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI = '1' THEN 'Yes'
                WHEN TITLEI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI = '1' THEN 'Yes'
                WHEN STITLI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS FLOAT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM {{ source('source_data', 'schools2014') }}

        UNION ALL

        SELECT
            SURVYEAR || '-' || (CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE || '-' || STID || '-' || SEASCH AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE || '-' || STID AS StateDistrictID
            ,GSLO AS LowGrade
            ,GSHI AS HighGrade
            ,SCHNAM AS SchoolName
            ,LEANM AS District
            ,CONAME AS CountyName
            ,LSTREE AS StreetAddress
            ,LCITY as City
            ,LSTATE AS State
            ,LZIP AS ZIP
            ,LZIP4 AS ZIP4
            ,PHONE AS Phone
            ,ULOCAL AS LocaleCode
            ,CASE 
                WHEN CHARTR = '1' THEN 'Yes' 
                WHEN CHARTR = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET = '1' THEN 'Yes'
                WHEN MAGNET = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI = '1' THEN 'Yes'
                WHEN TITLEI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI = '1' THEN 'Yes'
                WHEN STITLI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS FLOAT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM {{ source('source_data', 'schools2013') }}
         
        UNION ALL

        SELECT
            SURVYEAR || '-' || (CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE || '-' || STID || '-' || SEASCH AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE || '-' || STID AS StateDistrictID
            ,GSLO AS LowGrade
            ,GSHI AS HighGrade
            ,SCHNAM AS SchoolName
            ,LEANM AS District
            ,CONAME AS CountyName
            ,LSTREE AS StreetAddress
            ,LCITY as City
            ,LSTATE AS State
            ,LZIP AS ZIP
            ,LZIP4 AS ZIP4
            ,PHONE AS Phone
            ,ULOCAL AS LocaleCode
            ,CASE 
                WHEN CHARTR = '1' THEN 'Yes' 
                WHEN CHARTR = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET = '1' THEN 'Yes'
                WHEN MAGNET = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI = '1' THEN 'Yes'
                WHEN TITLEI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI = '1' THEN 'Yes'
                WHEN STITLI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS FLOAT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM {{ source('source_data', 'schools2012') }}

        UNION ALL

        SELECT
            SURVYEAR || '-' || (CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE || '-' || STID || '-' || SEASCH AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE || '-' || STID AS StateDistrictID
            ,GSLO AS LowGrade
            ,GSHI AS HighGrade
            ,SCHNAM AS SchoolName
            ,LEANM AS District
            ,CONAME AS CountyName
            ,LSTREE AS StreetAddress
            ,LCITY as City
            ,LSTATE AS State
            ,LZIP AS ZIP
            ,LZIP4 AS ZIP4
            ,PHONE AS Phone
            ,ULOCAL AS LocaleCode
            ,CASE 
                WHEN CHARTR = '1' THEN 'Yes' 
                WHEN CHARTR = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET = '1' THEN 'Yes'
                WHEN MAGNET = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI = '1' THEN 'Yes'
                WHEN TITLEI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI = '1' THEN 'Yes'
                WHEN STITLI = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS FLOAT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM {{ source('source_data', 'schools2011') }}

        UNION ALL

        SELECT
            '2009-2010' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE09 || '-' || STID09 || '-' || SEASCH09 AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE09 || '-' || STID09 AS StateDistrictID
            ,GSLO09 AS LowGrade
            ,GSHI09 AS HighGrade
            ,SCHNAM09 AS SchoolName
            ,LEANM09 AS District
            ,CONAME09 AS CountyName
            ,LSTREE09 AS StreetAddress
            ,LCITY09 as City
            ,LSTATE09 AS State
            ,LZIP09 AS ZIP
            ,LZIP409 AS ZIP4
            ,PHONE09 AS Phone
            ,ULOCAL09 AS LocaleCode
            ,CASE 
                WHEN CHARTR09 = '1' THEN 'Yes' 
                WHEN CHARTR09 = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET09 = '1' THEN 'Yes'
                WHEN MAGNET09 = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI09 = '1' THEN 'Yes'
                WHEN TITLEI09 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI09 = '1' THEN 'Yes'
                WHEN STITLI09 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER09 AS INT) >= 0 THEN MEMBER09 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE09 AS FLOAT) >= 0 THEN FTE09 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH09 AS INT) >= 0 THEN FRELCH09 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH09 AS INT) >= 0 THEN REDLCH09 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD09 As Latitude
            ,LONCOD09 as Longitude
        FROM {{ source('source_data', 'schools2010') }}

        UNION ALL 

        SELECT
            '2008-2009' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE08 || '-' || STID08 || '-' || SEASCH08 AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE08 || '-' || STID08 AS StateDistrictID
            ,GSLO08 AS LowGrade
            ,GSHI08 AS HighGrade
            ,SCHNAM08 AS SchoolName
            ,LEANM08 AS District
            ,CONAME08 AS CountyName
            ,LSTREE08 AS StreetAddress
            ,LCITY08 as City
            ,LSTATE08 AS State
            ,LZIP08 AS ZIP
            ,LZIP408 AS ZIP4
            ,PHONE08 AS Phone
            ,ULOCAL08 AS LocaleCode
            ,CASE 
                WHEN CHARTR08 = '1' THEN 'Yes' 
                WHEN CHARTR08 = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET08 = '1' THEN 'Yes'
                WHEN MAGNET08 = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI08 = '1' THEN 'Yes'
                WHEN TITLEI08 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI08 = '1' THEN 'Yes'
                WHEN STITLI08 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER08 AS INT) >= 0 THEN MEMBER08 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE08 AS FLOAT) >= 0 THEN FTE08 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH08 AS INT) >= 0 THEN FRELCH08 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH08 AS INT) >= 0 THEN REDLCH08 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD08 As Latitude
            ,LONCOD08 as Longitude
        FROM {{ source('source_data', 'schools2009') }}
         
        UNION ALL 

        SELECT
            '2007-2008' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,LSTATE07 || '-' || STID07 || '-' || SEASCH07 AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,LSTATE07 || '-' || STID07 AS StateDistrictID
            ,GSLO07 AS LowGrade
            ,GSHI07 AS HighGrade
            ,SCHNAM07 AS SchoolName
            ,LEANM07 AS District
            ,CONAME07 AS CountyName
            ,LSTREE07 AS StreetAddress
            ,LCITY07 as City
            ,LSTATE07 AS State
            ,LZIP07 AS ZIP
            ,LZIP407 AS ZIP4
            ,PHONE07 AS Phone
            ,ULOCAL07 AS LocaleCode
            ,CASE 
                WHEN CHARTR07 = '1' THEN 'Yes' 
                WHEN CHARTR07 = '2' THEN 'No' 
                ELSE NULL
            END AS Charter
            ,CASE
                WHEN MAGNET07 = '1' THEN 'Yes'
                WHEN MAGNET07 = '2' THEN 'No'
                ELSE NULL
            END AS Magnet
            ,CASE
                WHEN TITLEI07 = '1' THEN 'Yes'
                WHEN TITLEI07 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchool
            ,CASE
                WHEN STITLI07 = '1' THEN 'Yes'
                WHEN STITLI07 = '2' THEN 'No'
                ELSE NULL
            END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER07 AS INT) >= 0 THEN MEMBER07 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE07 AS FLOAT) >= 0 THEN FTE07 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH07 AS INT) >= 0 THEN FRELCH07 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH07 AS INT) >= 0 THEN REDLCH07 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD07 As Latitude
            ,LONCOD07 as Longitude
        FROM {{ source('source_data', 'schools2008') }}
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
        ,LocaleCode
        ,Charter
        ,Magnet
        ,TitleISchool
        ,TitleISchoolWide
        ,Students
        ,Teachers
        ,CAST(Students as FLOAT) / CAST(Teachers as FLOAT) AS StudentTeacherRatio
        ,FreeLunch
        ,ReducedLunch
        ,Latitude
        ,Longitude
    FROM T
