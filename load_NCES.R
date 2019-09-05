
#### This section is only needed when running interactively in R Studio

if(!grepl("C:/Program Files/R/", Sys.getenv("PATH"))) {
    Sys.setenv(PATH = paste(Sys.getenv("PATH"), "C:/Program Files/R/R-3.4.4/bin", sep=";"))
}

if (nchar(Sys.getenv("HADOOP_HOME")) < 1) {
    Sys.setenv(HADOOP_HOME = "C:/Users/jchiu/spark-2.4.3-bin-hadoop2.7")
}

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C:/Users/jchiu/spark-2.4.3-bin-hadoop2.7")
}

#### end section

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(readxl)


writeSparkTSV <- function(df, path) {
    # the key to correctly outputting NA values as unquoted empty strings is to use the "emptyValue" option
    # See https://spark.apache.org/docs/2.4.0/sql-migration-guide-upgrade.html
    # "To restore the previous behavior, set the CSV option emptyValue to empty (not quoted) string."
    write.df(df
        ,path = path
        ,source = "csv"
        ,mode = "overwrite"
        ,sep="\t"
        ,quote=""
        ,null_value = NA
        ,header = TRUE
        ,"emptyValue" = "")
}


sparkR.session(master = "local[8]", sparkConfig = list(spark.driver.memory = "2g"))

input_dir <- "C:/Users/jchiu/NCES/input"

num_partitions = 4

#### 2008 - 2014
#### these years had single files for CCD and variation in column names

# teacher count field (FTE) is lways -1 or -2 for WA state schools
createOrReplaceTempView(
    read.df(paste(input_dir, "sc132a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2014")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc122a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2013")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc111a_supp.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2012")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc102a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2011")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc102a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2011")

# in 2010 and prior, yr is encoded in the fieldname (e.g. SEASCH07)

createOrReplaceTempView(
    read.df(paste(input_dir, "sc092a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2010")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc081b.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2009")

createOrReplaceTempView(
    read.df(paste(input_dir, "sc071b.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"schools2008")

schools_2008_2014 <- sql("
    WITH T AS (
        SELECT
            CONCAT(SURVYEAR, '-', CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE, '-', STID, '-', SEASCH) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE, '-', STID) AS StateDistrictID
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
            ,CASE WHEN CHARTR = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS INT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM schools2014

        UNION ALL

        SELECT
            CONCAT(SURVYEAR, '-', CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE, '-', STID, '-', SEASCH) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE, '-', STID) AS StateDistrictID
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
            ,CASE WHEN CHARTR = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS INT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM schools2013
         
        UNION ALL

        SELECT
            CONCAT(SURVYEAR, '-', CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE, '-', STID, '-', SEASCH) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE, '-', STID) AS StateDistrictID
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
            ,CASE WHEN CHARTR = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS INT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM schools2012

        UNION ALL

        SELECT
            CONCAT(SURVYEAR, '-', CAST(SURVYEAR AS INT) + 1) AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE, '-', STID, '-', SEASCH) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE, '-', STID) AS StateDistrictID
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
            ,CASE WHEN CHARTR = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER AS INT) >= 0 THEN MEMBER ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE AS INT) >= 0 THEN FTE ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH AS INT) >= 0 THEN FRELCH ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH AS INT) >= 0 THEN REDLCH ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD As Latitude
            ,LONCOD as Longitude
        FROM schools2011

        UNION ALL

        SELECT
            '2009-2010' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE09, '-', STID09, '-', SEASCH09) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE09, '-', STID09) AS StateDistrictID
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
            ,CASE WHEN CHARTR09 = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET09 = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI09 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI09 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER09 AS INT) >= 0 THEN MEMBER09 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE09 AS INT) >= 0 THEN FTE09 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH09 AS INT) >= 0 THEN FRELCH09 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH09 AS INT) >= 0 THEN REDLCH09 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD09 As Latitude
            ,LONCOD09 as Longitude
        FROM schools2010

        UNION ALL 

        SELECT
            '2008-2009' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE08, '-', STID08, '-', SEASCH08) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE08, '-', STID08) AS StateDistrictID
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
            ,CASE WHEN CHARTR08 = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET08 = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI08 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI08 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER08 AS INT) >= 0 THEN MEMBER08 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE08 AS INT) >= 0 THEN FTE08 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH08 AS INT) >= 0 THEN FRELCH08 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH08 AS INT) >= 0 THEN REDLCH08 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD08 As Latitude
            ,LONCOD08 as Longitude
        FROM schools2009
         
        UNION ALL 

        SELECT
            '2007-2008' AS AcademicYear
            ,NCESSCH AS NCESSchoolID
            ,CONCAT(LSTATE07, '-', STID07, '-', SEASCH07) AS StateSchoolID
            ,LEAID AS NCESDistrictID
            ,CONCAT(LSTATE07, '-', STID07) AS StateDistrictID
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
            ,CASE WHEN CHARTR07 = '1' THEN 'Yes' ELSE 'No' END AS Charter
            ,CASE WHEN MAGNET07 = '1' THEN 'Yes' ELSE 'No' END AS Magnet
            ,CASE WHEN TITLEI07 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchool
            ,CASE WHEN STITLI07 = '1' THEN 'Yes' ELSE 'No' END AS TitleISchoolWide
            ,CASE WHEN CAST(MEMBER07 AS INT) >= 0 THEN MEMBER07 ELSE NULL END AS Students
            ,CASE WHEN CAST(FTE07 AS INT) >= 0 THEN FTE07 ELSE NULL END AS Teachers
            ,CASE WHEN CAST(FRELCH07 AS INT) >= 0 THEN FRELCH07 ELSE NULL END AS FreeLunch
            ,CASE WHEN CAST(REDLCH07 AS INT) >= 0 THEN REDLCH07 ELSE NULL END AS ReducedLunch
            -- additional fields not in School Locator format
            ,LATCOD07 As Latitude
            ,LONCOD07 as Longitude
        FROM schools2008
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
")

createOrReplaceTempView(
    schools_2008_2014
    ,"schools_2008_2014")

#### 2015 Onwards
#### these years had 5 separate files plus geocode file

#### directory file

createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_029_1617_w_1a_11212017.csv", sep="/"), source="csv", "header"= "true")
    ,"directory2017")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_029_1516_w_2a_011717.csv", sep="/"), source="csv", "header"= "true")
    ,"directory2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_029_1415_w_0216601a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"directory2015")

directory <- sql("
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
    FROM directory2017
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
    FROM directory2016
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
    FROM directory2015
    ")

repartition(directory, numPartitions = num_partitions)

createOrReplaceTempView(directory, "directory")

#### characteristics file

createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_129_1617_w_1a_11212017.csv", sep="/"), source="csv", "header"= "true")
    ,"characteristics2017")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_129_1516_w_2a_011717.csv", sep="/"), source="csv", "header"= "true")
    ,"characteristics2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_129_1415_w_0216161a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"characteristics2015")

characteristics <- sql("
    SELECT
        SCHOOL_YEAR
        ,NCESSCH
        ,MAGNET_TEXT
        ,TITLEI_STATUS 
    FROM characteristics2017
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,MAGNET_TEXT
        ,TITLEI_STATUS 
    FROM characteristics2016
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,MAGNET_TEXT
        ,TITLEI_STATUS 
    FROM characteristics2015
    ")

characteristics <- repartition(characteristics, numPartitions = num_partitions)

createOrReplaceTempView(characteristics, "characteristics")

#### staff file

createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_059_1617_l_2a_11212017_csv.csv", sep="/"), source="csv", "header"= "true")
    ,"staff2017")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_059_1516_w_2a_011717.csv", sep="/"), source="csv", "header"= "true")
    ,"staff2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_059_1415_w_0216161a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"staff2015")

staff <- sql("
    SELECT
        SCHOOL_YEAR
        ,NCESSCH
        ,TEACHERS
    FROM staff2017
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,FTE
    FROM staff2016
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,FTE
    FROM staff2015
    ")

staff <- repartition(staff, numPartitions = num_partitions)

createOrReplaceTempView(staff, "staff")

#### membership file

createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_SCH_052_1617_l_2a_11212017.csv", sep="/"), source="csv", "header"= "true")
    ,"membership2017")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_052_1516_w_2a_011717.csv", sep="/"), source="csv", "header"= "true")
    ,"membership2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_052_1415_w_0216161a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"membership2015")

membership <- sql("
    SELECT
        SCHOOL_YEAR
        ,NCESSCH
        ,STUDENT_COUNT
    FROM membership2017
    WHERE TOTAL_INDICATOR = 'Derived - Education Unit Total minus Adult Education Count'
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,MEMBER AS STUDENT_COUNT
    FROM membership2016
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,MEMBER AS STUDENT_COUNT
    FROM membership2015
    ")

membership <- repartition(membership, numPartitions = num_partitions)

createOrReplaceTempView(membership, "membership")

#### lunch file

createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_033_1617_l_2a_11212017.csv", sep="/"), source="csv", "header"= "true")
    ,"lunch2017")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_033_1516_w_2a_011717.csv", sep="/"), source="csv", "header"= "true")
    ,"lunch2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "ccd_sch_033_1415_w_0216161a.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"lunch2015")

free_lunch <- sql("
    SELECT
        SCHOOL_YEAR
        ,NCESSCH
        ,STUDENT_COUNT
    FROM lunch2017
    WHERE TOTAL_INDICATOR = 'Category Set A' AND LUNCH_PROGRAM = 'Free lunch qualified'
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,FRELCH AS STUDENT_COUNT
    FROM lunch2016
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,FRELCH AS STUDENT_COUNT
    FROM lunch2015
    ")

createOrReplaceTempView(free_lunch, "free_lunch")

reduced_lunch <- sql("
    SELECT
        SCHOOL_YEAR
        ,NCESSCH
        ,STUDENT_COUNT
    FROM lunch2017
    WHERE TOTAL_INDICATOR = 'Category Set A' AND LUNCH_PROGRAM = 'Reduced-price lunch qualified'
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,REDLCH AS STUDENT_COUNT
    FROM lunch2016
    UNION ALL
    SELECT
        SURVYEAR AS SCHOOL_YEAR
        ,NCESSCH
        ,REDLCH AS STUDENT_COUNT
    FROM lunch2015
    ")

createOrReplaceTempView(reduced_lunch, "reduced_lunch")

#### geocode

geocodeRaw2017 <- read.df(paste(input_dir, "EDGE_GEOCODE_PUBLICSCH_1617/EDGE_GEOCODE_PUBLICSCH_1617.TXT", sep="/"), source="csv", sep="|")
colnames(geocodeRaw2017) <- c(
  "NCESSCH", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP", "STFIP", "CNTY",
  "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA", "CBSATYPE", "CSA", "NMCSA", "NECTA",
  "NMNECTA", "CD", "SLDL", "SLDU", "SCHOOLYEAR" )

createOrReplaceTempView(
    geocodeRaw2017
    ,"geocodeRaw2017")
# createOrReplaceTempView(
#     createDataFrame(read_excel(paste(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.xlsx", sep="/")))
#     ,"geocodeRaw2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"geocodeRaw2016")
createOrReplaceTempView(
    read.df(paste(input_dir, "EDGE_GEOIDS_201415_PUBLIC_SCHOOL.csv", sep="/"), source="csv", "header"= "true")
    ,"geocodeRaw2015")

geocode <- sql("
    SELECT
        CONCAT(SCHOOLYEAR, '-', CAST(SCHOOLYEAR AS INT) + 1) AS SCHOOLYEAR
        ,NCESSCH
        ,NMCNTY
        ,LOCALE
        ,LAT
        ,LON
    FROM geocodeRaw2017
    UNION ALL
    SELECT
        '2015-2016' AS SCHOOLYEAR
        ,NCESSCH
        ,NMCNTY15
        ,LOCALE15
        ,LAT1516
        ,LON1516
    FROM geocodeRaw2016
    UNION ALL
    SELECT
        CONCAT(SURVYEAR, '-', CAST(SURVYEAR AS INT) + 1) AS SCHOOLYEAR
        ,NCESSCH
        ,CONAME
        ,LOCALE
        ,LATCODE
        ,LONGCODE
    FROM geocodeRaw2015
    ")

createOrReplaceTempView(geocode, "geocode")

schools_2015_onwards <- sql("
    WITH T AS (
        SELECT
          	d.SCHOOL_YEAR AS AcademicYear
            ,d.NCESSCH AS NCESSchoolID
            ,CASE 
                WHEN d.SCHOOL_YEAR = '2015-2016' THEN CONCAT(LSTATE, '-', d.ST_SCHID) 
                WHEN CAST(SUBSTRING(d.SCHOOL_YEAR, 6, 4) as INT) <= 2015 THEN CONCAT(LSTATE, '-', d.ST_LEAID, '-', d.ST_SCHID)
                ELSE d.ST_SCHID
            END AS StateSchoolID
            ,d.LEAID AS NCESDistrictID
            ,CASE 
                WHEN CAST(SUBSTRING(d.SCHOOL_YEAR, 6, 4) as INT) <= 2016 THEN CONCAT(LSTATE, '-', d.ST_LEAID)
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
            ,d.CHARTER_TEXT AS Charter
            ,CASE
            	WHEN c.MAGNET_TEXT = 'Yes' THEN 'Yes'
            	ELSE 'No'
            END AS Magnet
            ,CASE
            	WHEN c.TITLEI_STATUS IN ('SWELIGNOPROG', 'SWELIGTGPROG', 'TGELGBNOPROG', 'TGELGBTGPROG') THEN 'Yes'
            	ELSE 'No'
            END AS TitleISchool
            ,CASE
            	WHEN c.TITLEI_STATUS = 'SWELIGSWPROG' THEN 'Yes'
            	ELSE 'No'
            END AS TitleISchoolWide
            ,CASE WHEN CAST(m.STUDENT_COUNT as INT) >= 0 THEN m.STUDENT_COUNT ELSE NULL END AS Students
            ,CASE WHEN CAST(s.TEACHERS as INT) >= 0 THEN s.TEACHERS ELSE NULL END AS Teachers
            ,free.STUDENT_COUNT AS FreeLunch
            ,reduced.STUDENT_COUNT AS ReducedLunch
            -- additional fields not in School Locator format
            ,g.LAT As Latitude
            ,g.LON as Longitude
        FROM directory d
        LEFT JOIN characteristics c
            ON d.SCHOOL_YEAR = c.SCHOOL_YEAR
            AND d.NCESSCH = c.NCESSCH
        LEFT JOIN staff s
         	ON d.SCHOOL_YEAR = s.SCHOOL_YEAR
        	AND d.NCESSCH = s.NCESSCH
        LEFT JOIN membership m
         	ON d.SCHOOL_YEAR = m.SCHOOL_YEAR
        	AND d.NCESSCH = m.NCESSCH
        LEFT JOIN free_lunch free
         	ON d.SCHOOL_YEAR = free.SCHOOL_YEAR
        	AND d.NCESSCH = free.NCESSCH
        LEFT JOIN reduced_lunch reduced
         	ON d.SCHOOL_YEAR = reduced.SCHOOL_YEAR
        	AND d.NCESSCH = reduced.NCESSCH
        LEFT JOIN geocode g
         	ON d.SCHOOL_YEAR = g.SCHOOLYEAR
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
    ")

createOrReplaceTempView(
    schools_2015_onwards
    ,"schools_2015_onwards")

#### Locale

createOrReplaceTempView(
    read.df(paste(input_dir, "LocaleCodes.txt", sep="/"), source="csv", "header"= "true", sep="\t")
    ,"locale_codes")


#### Union everything

final <- sql("
    WITH T AS (
        SELECT
        *
        FROM schools_2008_2014

        UNION ALL

        SELECT
        *
        FROM schools_2015_onwards
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
        ,Students
        ,Teachers
        ,StudentTeacherRatio
        ,FreeLunch
        ,ReducedLunch
        ,Latitude
        ,Longitude
    FROM T
    LEFT JOIN locale_codes lc
        ON T.LocaleCode = lc.LocaleCode
 ")

writeSparkTSV(final, "output")

sparkR.session.stop()
