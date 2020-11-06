
# NCES ETL

Transforms the [NCES Common Core of Data](https://nces.ed.gov/ccd/) set of
archival files (2001 - present) into the convenient output format of the
[School Locator](https://nces.ed.gov/ccd/schoolsearch/) tool (which only
provides data from the latest year). Output includes some helpful
tweaks/additions (namely, latitude and longitude fields).

## Requirements

- Windows 10
- Python >= 3.8.3

Should work on Linux if you puzzle through what the PowerShell code does and figure out the equivalent things to run on your system.

## Setup

Create a text file in your home directory at location: `.dbt/profiles.yml` with the following lines:

```
nces:

  outputs:

    dev:
      type: sqlite
      threads: 1
      database: "database"
      schema: "main"
      schemas_and_paths: "main=C:/Users/jchiu/NCES/database/nces.db"
      schema_directory: "main=C:/Users/jchiu/NCES"

  target: dev
```

Run this in PowerShell:

```sh
#  change to repo dir
cd ~/whenever_this_repo_is

# create a virtualenv and activate it
python -m venv env
.\env\Scripts\activate.ps1

# install packages
pip install -r requirements.txt
```

Now you'e ready to go.

## Running the ETL

```sh
# download all the input files; safe to run repeatedly, will only download what's missing
python download.py

# load the input files into sqlite
# change 'input_dir' in the script to use files on network drive
python load.py

# create models
dbt run

# create exports
python export.py
```

Downloading and loading take the most time. Creating the models is pretty fast.

## Notes

The output file very closely resembles the output produced by the [NCES School Locator](https://nces.ed.gov/ccd/schoolsearch/) tool.
As such, it has the following fields:

```
AcademicYear
NCESSchoolID
StateSchoolID
NCESDistrictID
StateDistrictID
LowGrade
HighGrade
SchoolName
District
CountyName
StreetAddress
City
State
ZIP
ZIP4
Phone
LocaleCode
Locale
Charter
Magnet
TitleISchool
TitleISchoolWide
Students
Teachers
StudentTeacherRatio
FreeLunch
ReducedLunch
Latitude
Longitude
```

AcademicYear values below are based on the "end year": e.g. 2016 means 2015-2016.

Earliest year loaded is 2001. Prior to that, there are no geolocation fields,
so they aren't very useful for our purposes. But the code could be extended to
load earlier data.

Note that there are inaccuracies/missing values for various year/district/school
combinations. In particular:
- Numbers for FreeLunch in 2005 and 2007 are abnormally low compared to other years

'Missing' and 'Not applicable' are converted to NULLs.

## Sanity Checks and Validation

Find any 'bad' records, usually due to parsing problems in early years,
when files have fixed-length records.

```
select *
from final where
    not (charter is null OR charter in ('Yes', 'No'))
    or not (magnet is null OR Magnet in ('Yes', 'No'))
    or not (TitleISchool is null OR TitleISchool in ('Yes', 'No'))
    or not (TitleISchoolWide is null OR TitleISchoolWide in ('Yes', 'No'))
    OR (students is not null and cast(students as float) < 0.0)
    OR (teachers is not null and cast(teachers as float) < 0.0)
    OR (freelunch is not null and cast(freelunch as float) < 0.0)
    or (reducedlunch is not null and cast(Reducedlunch as float) < 0.0)

```

These counts shouldn't shouldn't significantly change from year to year.

```
WITH t AS (
    SELECT
        AcademicYear, 
        case when cast(Students as int) > 0 then 1 else 0 end as HasStudents,
        case when cast(Teachers as float) > 0 then 1 else 0 end as HasTeachers,
        case when TitleISchool = 'Yes' THEN 1 ELSE 0 END AS TitleISchool,
        case when TitleISchoolWide = 'Yes' THEN 1 ELSE 0 END AS TitleISchoolWide,
        case when Magnet = 'Yes' THEN 1 ELSE 0 END AS Magnet,
        case when Charter = 'Yes' THEN 1 ELSE 0 END AS Charter,
        case when cast(FreeLunch as int) > 0 then 1 else 0 end as HasFreeLunch,
        case when cast(ReducedLunch as int) > 0 then 1 else 0 end as HasReducedLunch,
        case when Latitude is not null then 1 else 0 end as HasLatLng
    FROM final WHERE State = 'WA'
)
select 
    AcademicYear
    ,count(*) as Total
	,sum(HasStudents) AS HasStudents
	,sum(HasTeachers) AS HasTeachers
    ,sum(TitleISchool) as TitleISchool
    ,cast(sum(TitleISchool) as FLOAT) / count(*) as TitleISchoolPct
    ,sum(TitleISchoolWide) as TitleISchoolWide
    ,cast(sum(TitleISchoolWide) as FLOAT) / count(*) as TitleISchoolWidePct
    ,sum(Magnet) as Magnet
    ,cast(sum(Magnet) as FLOAT) / count(*) as MagnetPct
    ,sum(Charter) as Charter
    ,cast(sum(Charter) as FLOAT) / count(*) as CharterPct
    ,sum(HasFreeLunch) as HasFreeLunch
    ,cast(sum(HasFreeLunch) as FLOAT) / count(*) as HasFreeLunchPct
    ,sum(HasReducedLunch) as HasReducedLunch
    ,cast(sum(HasReducedLunch) as FLOAT) / count(*) as HasReducedLunchPct
    ,sum(HasLatLng) as HasLatLng
    ,cast(sum(HasLatLng) as FLOAT) / count(*) as HasLatLngPct
from t
group by AcademicYear
order by AcademicYear
```
