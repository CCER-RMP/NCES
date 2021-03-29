
import csv
import os.path
import subprocess
import zipfile

import openpyxl
import requests

# change this to use files stored on S:
#input_dir = "S:/Data/Data System/RawSourceFiles/NCES/Common Core of Data"
input_dir = os.path.abspath("./input")

urls = [
    # from 2015 to 2018, there are 5 files in common core, and a separate geocode file

    # these are school-level files

    # to find these files via the website, go to this URL:
    # https://nces.ed.gov/ccd/files.asp
    # select 'Nonfiscal' and 'School' for the level.
    # the geocode file is separate:
    # https://nces.ed.gov/programs/edge/Geographic/SchoolLocations

    # three-char codes in the filenames:
    # 029 = directory file
    # 052 = membership file
    # 059 = staff file
    # 129 = school characteristics file
    # 033 = lunch program accessibility

    # 2020
    "https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1920_w_1a_082120.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_SCH_052_1920_l_1a_082120.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1920_l_1a_082120.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1920_w_1a_082120.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1920_l_1a_082120.zip"
    # geocode
    ,"https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1920.zip"

    # 2019
    ,"https://nces.ed.gov/ccd/data/zip/ccd_sch_029_1819_w_1a_091019.zip"
    ,"https://nces.ed.gov/ccd/data/zip/ccd_sch_052_1819_l_1a_091019.zip"
    ,"https://nces.ed.gov/ccd/data/zip/ccd_sch_059_1819_l_1a_091019.zip"
    ,"https://nces.ed.gov/ccd/data/zip/ccd_sch_129_1819_w_1a_091019.zip"
    ,"https://nces.ed.gov/ccd/data/zip/ccd_sch_033_1819_l_1a_091019.zip"
    # geocode
    ,"https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1819.zip"

    # 2018
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1718_w_1a_083118.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1718_l_1a_083118.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1718_l_1a_083118.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1718_w_1a_083118.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1718_l_1a_083118.zip"
    # geocode
    ,"https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1718.zip"

    # 2017
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1617_w_1a_11212017_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_SCH_052_1617_l_2a_11212017_CSV.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1617_l_2a_11212017_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1617_w_1a_11212017_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1617_l_2a_11212017_csv.zip"
    # geocode
    ,"https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1617.zip"

    # 2016
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1516_w_2a_011717_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1516_w_2a_011717_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1516_w_2a_011717_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1516_w_2a_011717_csv.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1516_w_2a_011717_csv.zip"
    # geocode
    ,"https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1516.zip"

    # 2015
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1415_w_0216601a_txt.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1415_w_0216161a_txt.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1415_w_0216161a_txt.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1415_w_0216161a_txt.zip"
    ,"https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1415_w_0216161a_txt.zip"
    # last year that geocode data is provided as part of common core data; after this, it's on EDGE page
    ,"https://nces.ed.gov/ccd/Data/zip/EDGE_GEOIDS_201415_PUBLIC_SCHOOL_csv.zip"

    # 2014
    ,"https://nces.ed.gov/ccd/Data/zip/sc132a_txt.zip"
    # 2013
    ,"https://nces.ed.gov/ccd/Data/zip/sc122a_txt.zip"
    # 2012
    ,"https://nces.ed.gov/ccd/Data/zip/sc111a_supp_txt.zip"
    # 2011
    ,"https://nces.ed.gov/ccd/Data/zip/sc102a_txt.zip"
    # 2010
    ,"https://nces.ed.gov/ccd/data/zip/sc092a_txt.zip"
    # 2009
    ,"https://nces.ed.gov/ccd/data/zip/sc081b_txt.zip"
    # 2008
    ,"https://nces.ed.gov/ccd/data/zip/sc071b_txt.zip"

    # 2007
    ,"https://nces.ed.gov/ccd/data/zip/sc061cai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc061ckn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc061cow_dat.zip"

    # 2006
    ,"https://nces.ed.gov/ccd/data/zip/sc051aai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc051akn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc051aow_dat.zip"

    # 2005
    ,"https://nces.ed.gov/ccd/data/zip/sc041bai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc041bkn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc041bow_dat.zip"

    # 2004
    ,"https://nces.ed.gov/ccd/data/zip/sc031aai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc031akn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc031aow_dat.zip"

    # 2003
    ,"https://nces.ed.gov/ccd/data/zip/sc021aai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc021akn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc021aow_dat.zip"

    # 2002
    ,"https://nces.ed.gov/ccd/data/zip/sc011aai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc011akn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc011aow_dat.zip"

    # 2001
    ,"https://nces.ed.gov/ccd/data/zip/sc001aai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc001akn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc001aow_dat.zip"

    # 2000
    ,"https://nces.ed.gov/ccd/data/zip/sc991bai_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc991bkn_dat.zip"
    ,"https://nces.ed.gov/ccd/data/zip/sc991bow_dat.zip"

]


def empty_str_to_none(s):
    return None if s == '' else s


def unzip(path, target_dir):
    # with zipfile.ZipFile(path, 'r') as zip_ref:
    #     zip_ref.extractall(target_dir)

    # built-in zipfile library chokes on some files so use unzip.exe instead
    subprocess.run(['C:/Program Files/Git/usr/bin/unzip.exe',
        '-o',
        path,
        '-d',
        target_dir
        ], check=True)


for url in urls:
    base = url[url.rindex('/') + 1:]
    path = os.path.join(input_dir, base)

    print(path)

    if not os.path.exists(path):
        print(f"Downloading {url}")
        r = requests.get(url)
        with open(path, 'wb') as f:
            f.write(r.content)

        print(f"Unzipping {path}")
        unzip(path, input_dir)
    else:
        print(f"Skipping {url}")

#### these are zip files within zip files

unzip(os.path.join(input_dir, "ccd_SCH_052_1718_l_1a_083118 CSV.zip"), input_dir)

# Nov 2020: NCES seems to have updated this file to rm the nested zips,
# so commenting this out.
#unzip(os.path.join(input_dir, "ccd_SCH_052_1819_l_1a_091019_CSV.zip"), input_dir)

geocode2016 = os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.txt")

if not os.path.exists(geocode2016):
    print("Converting geocode file for 2016 to TXT...")

    workbook = openpyxl.load_workbook(filename=os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.xlsx"), read_only=True)
    sheet = workbook.active
    rows = sheet.values

    with open(geocode2016, "w") as output_file:
        for row in rows:
            output_file.write("\t".join([empty_str_to_none(str(value)) for value in row]))
            output_file.write("\n")
