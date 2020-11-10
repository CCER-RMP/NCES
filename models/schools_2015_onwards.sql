
WITH directoryModified as (
    SELECT
        *
        -- example code for joining on previous year when only preliminary data is available.
        -- we should probably never do this: it's misleading. any 'patching' of this sort should happen further downstream.
        --,CASE WHEN SCHOOL_YEAR = '2018-2019' THEN '2017-2018' ELSE SCHOOL_YEAR END AS SCHOOL_YEAR_FOR_JOIN
        ,SCHOOL_YEAR AS SCHOOL_YEAR_FOR_JOIN
    FROM {{ ref('directory') }}
)
,T AS (
    SELECT
        d.SCHOOL_YEAR AS AcademicYear
        ,d.NCESSCH AS NCESSchoolID
        ,CASE 
            WHEN d.SCHOOL_YEAR = '2015-2016' THEN LSTATE || '-' || d.ST_SCHID
            WHEN CAST(SUBSTR(d.SCHOOL_YEAR, 6, 4) as INT) <= 2015 THEN LSTATE || '-' || d.ST_LEAID || '-' || d.ST_SCHID
            ELSE d.ST_SCHID
        END AS StateSchoolID
        ,d.LEAID AS NCESDistrictID
        ,CASE 
            WHEN CAST(SUBSTR(d.SCHOOL_YEAR, 6, 4) as INT) <= 2016 THEN LSTATE || '-' || d.ST_LEAID
            ELSE d.ST_LEAID
        END AS StateDistrictID
        ,d.GSLO AS LowGrade
        ,d.GSHI AS HighGrade
        ,d.SCH_NAME AS SchoolName
        ,d.LEA_NAME AS District
        ,g.NMCNTY AS CountyName
        ,d.LSTREET1 AS StreetAddress
        ,d.LCITY as City
        ,d.LSTATE AS State
        ,d.LZIP AS ZIP
        ,d.LZIP4 AS ZIP4
        ,d.PHONE AS Phone
        ,g.LOCALE AS LocaleCode
        ,CASE
            WHEN d.CHARTER_TEXT IN ('Yes', 'No') THEN d.CHARTER_TEXT
            ELSE NULL
        END AS Charter
        ,CASE
            WHEN c.MAGNET_TEXT IN ('Yes', 'No') THEN c.MAGNET_TEXT
            ELSE NULL
        END AS Magnet
        ,CASE
            WHEN d.SCHOOL_YEAR IN ('2014-2015', '2015-2016') THEN
                CASE
                    WHEN c.TITLEI_STATUS IN ('1', '2', '3', '4', '5') THEN 'Yes'
                    WHEN c.TITLEI_STATUS IN ('6') THEN 'No'
                    ELSE NULL
                END 
            ELSE
                CASE
                    WHEN c.TITLEI_STATUS IN ('SWELIGNOPROG', 'SWELIGSWPROG', 'SWELIGTGPROG', 'TGELGBNOPROG', 'TGELGBTGPROG') THEN 'Yes'
                    WHEN c.TITLEI_STATUS IN ('NOTTITLE1ELIG') THEN 'No'
                    ELSE NULL
                END
        END AS TitleISchool
        -- TODO: School Locator has 'not applicable' values for School Wide; not sure what the logic is for this. we set it to No.
        ,CASE
            WHEN d.SCHOOL_YEAR IN ('2014-2015', '2015-2016') THEN
                CASE
                    WHEN c.TITLEI_STATUS IN ('3', '4', '5') THEN 'Yes'
                    WHEN c.TITLEI_STATUS IN ('6') THEN 'No'
                    ELSE NULL
                END 
            ELSE
                CASE
                    WHEN c.TITLEI_STATUS IN ('SWELIGNOPROG', 'SWELIGSWPROG', 'SWELIGTGPROG') THEN 'Yes'
                    WHEN c.TITLEI_STATUS IN ('TGELGBNOPROG', 'TGELGBTGPROG', 'NOTTITLE1ELIG') THEN 'No'
                    ELSE NULL
                END
        END AS TitleISchoolWide
        ,CASE WHEN CAST(m.STUDENT_COUNT as INT) >= 0 THEN m.STUDENT_COUNT ELSE NULL END AS Students
        ,CASE WHEN CAST(s.TEACHERS as FLOAT) >= 0 THEN s.TEACHERS ELSE NULL END AS Teachers
        ,CASE WHEN CAST(free.STUDENT_COUNT AS INT) >= 0 THEN free.STUDENT_COUNT ELSE NULL END AS FreeLunch
        ,CASE WHEN CAST(reduced.STUDENT_COUNT AS INT) >= 0 THEN reduced.STUDENT_COUNT ELSE NULL END AS ReducedLunch
        -- additional fields not in School Locator format
        ,g.LAT As Latitude
        ,g.LON as Longitude
    FROM directoryModified d
    LEFT JOIN {{ ref('characteristics') }} c
        ON d.SCHOOL_YEAR_FOR_JOIN = c.SCHOOL_YEAR
        AND d.NCESSCH = c.NCESSCH
    LEFT JOIN {{ ref('staff') }}  s
        ON d.SCHOOL_YEAR_FOR_JOIN = s.SCHOOL_YEAR
        AND d.NCESSCH = s.NCESSCH
    LEFT JOIN {{ ref('membership') }} m
        ON d.SCHOOL_YEAR_FOR_JOIN = m.SCHOOL_YEAR
        AND d.NCESSCH = m.NCESSCH
    LEFT JOIN {{ ref('free_lunch') }} free
        ON d.SCHOOL_YEAR_FOR_JOIN = free.SCHOOL_YEAR
        AND d.NCESSCH = free.NCESSCH
    LEFT JOIN {{ ref('reduced_lunch') }} reduced
        ON d.SCHOOL_YEAR_FOR_JOIN = reduced.SCHOOL_YEAR
        AND d.NCESSCH = reduced.NCESSCH
    LEFT JOIN {{ ref('geocode') }} g
        ON d.SCHOOL_YEAR_FOR_JOIN = g.SCHOOLYEAR
        AND d.NCESSCH = g.NCESSCH
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
