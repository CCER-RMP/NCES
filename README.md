
# NCES ETL

Transforms the [NCES Common Core of Data](https://nces.ed.gov/ccd/) set of
archival files into the convenient output format of the
[School Locator](https://nces.ed.gov/ccd/schoolsearch/) tool, with some
helpful tweaks/additions (namely, latitude and longitude fields)

:warning: This is a WIP! :warning:

# Requirements

- Windows 10 and PowerShell
- R 3.4.4
- Java 8
- Apache Spark (instructions below)

# Installing Spark

- Download `spark-2.4.3-bin-hadoop2.7.tgz`
- Unpack it to your home dir: e.g. `C:/Users/jchiu/spark-2.4.3-bin-hadoop2.7`
- Download `winutils.exe` for hadoop 2.7.3 from [here](https://github.com/cdarlint/winutils)
and put it into `C:/Users/jchiu/spark-2.4.3-bin-hadoop2.7/bin`

# Running the code

- Start a PowerShell window and import this module:

```
cd NCES
Import-Module -Force .\NCES_ETL
```

- Install required R libraries (you only need to run this once)

```
Install-NCESRLibraries
```

- Download NCES files and unpack them into the `input/` directory:

```
Get-NCESData
```

- Run the ETL to create files in `output/` directory:

```
Invoke-NCESETL
```

# Notes

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
