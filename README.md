
# NCES ETL

Transforms the [NCES Common Core of Data](https://nces.ed.gov/ccd/) set of
archival files into the more convenient format of the
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

- Download NCES files and unpack them into the `input/` directory:

```
Get-NCESData
```

- Run the ETL to create files in `output/` directory:

```
Invoke-NCESETL
```
