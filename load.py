
import codecs
import csv
import os.path
import sqlite3

import openpyxl

def empty_str_to_none(s):
    return None if s == '' else s


def clean_column_name(name):
    """
    deal with columns that conflict with reserved SQL keywords
    """
    if name in ["UNION", "AS"]:
        return name + "_"
    return name


def create_db_table(conn, table, headers):
    cursor = conn.cursor()

    field_list = ",".join([header + " text" for header in headers])
    cursor.execute(f'DROP TABLE IF EXISTS {table}')
    cursor.execute(f'CREATE TABLE {table} ( {field_list} )')

    conn.commit()


def load_into_sqlite(conn, paths, table, columns=None, create_table=False, delimiter=","):

    cursor = conn.cursor()

    table_created = False

    for path in paths:
        print("Loading %s" % (path,))

        count = 0

        with codecs.open(path, 'r', 'latin1') as f:
            reader = csv.reader(f, delimiter=delimiter)

            if columns:
                headers = columns
            else:
                headers = next(reader)

            headers = [clean_column_name(column_name) for column_name in headers]

            if create_table and not table_created:
                create_db_table(conn, table, headers)
                table_created = True

            num_rows_loaded = load_from_iterator(conn, reader, table, len(headers))
            count = count + num_rows_loaded

        print(f"{count} rows loaded from {path}")


def load_from_iterator(conn, iter, table, row_size):

    cursor = conn.cursor()

    count = 0
    eof = False

    while not eof:
        batch = []
        for i in range(100000):
            try:
                row = next(iter)
                # TODO: process the row
                row = [empty_str_to_none(value) for value in row]
                batch.append(row)
            except StopIteration:
                eof = True
                break  # Iterator exhausted: stop the loop

        if len(batch) > 0:
            print("Writing batch to staging db...")
            cursor.executemany('INSERT INTO %s VALUES (%s)' % (table, ",".join(["?" for x in range(row_size)])), batch)
            conn.commit()
            count = count + len(batch)

    return count


input_dir = "./input"

conn = sqlite3.connect('database/NCES.db')

#### 2007 and prior

# these files have fixed-length fields, and the starting positions/lengths of fields vary a lot
# across years

load_into_sqlite(conn, [
    os.path.join(input_dir, "Sc061cai.dat"),
    os.path.join(input_dir, "Sc061ckn.dat"),
    os.path.join(input_dir, "Sc061cow.dat"),
], "schools2007", columns=['LINE'], create_table=True, delimiter="\n")


load_into_sqlite(conn, [
    os.path.join(input_dir, "Sc051aai.dat"),
    os.path.join(input_dir, "Sc051akn.dat"),
    os.path.join(input_dir, "Sc051aow.dat"),
], "schools2006", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc041bai.dat"),
    os.path.join(input_dir, "sc041bkn.dat"),
    os.path.join(input_dir, "sc041bow.dat"),
], "schools2005", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc031aai.txt"),
    os.path.join(input_dir, "sc031akn.txt"),
    os.path.join(input_dir, "sc031aow.txt"),
], "schools2004", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "Sc021aai.txt"),
    os.path.join(input_dir, "Sc021akn.txt"),
    os.path.join(input_dir, "Sc021aow.txt"),
], "schools2003", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc011aai.dat"),
    os.path.join(input_dir, "sc011akn.dat"),
    os.path.join(input_dir, "sc011aow.dat"),
], "schools2002", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc001aai.dat"),
    os.path.join(input_dir, "sc001akn.dat"),
    os.path.join(input_dir, "sc001aow.dat"),
], "schools2001", columns=['LINE'], create_table=True, delimiter="\n")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc991bai.dat"),
    os.path.join(input_dir, "sc991bkn.dat"),
    os.path.join(input_dir, "sc991bow.dat"),
], "schools2000", columns=['LINE'], create_table=True, delimiter="\n")

#### 2008 - 2014

# these years had single files for CCD

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc132a.txt"),
], "schools2014", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc122a.txt"),
], "schools2013", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc111a_supp.txt"),
], "schools2012", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc102a.txt"),
], "schools2011", create_table=True, delimiter="\t")

# in 2010 and prior, yr is encoded in the fieldname (e.g. SEASCH07)

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc092a.txt"),
], "schools2010", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc081b.txt"),
], "schools2009", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "sc071b.txt"),
], "schools2008", create_table=True, delimiter="\t")


#### 2015 Onwards
#### these years had 5 separate files plus geocode file

#### directory file

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_029_1819_w_1a_091019.csv"),
], "directory2019", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_029_1718_w_1a_083118.csv"),
], "directory2018", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_029_1617_w_1a_11212017.csv"),
], "directory2017", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_029_1516_w_2a_011717.csv"),
], "directory2016", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_029_1415_w_0216601a.txt"),
], "directory2015", create_table=True, delimiter="\t")

# #### characteristics file

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_129_1819_w_1a_091019.csv"),
], "characteristics2019", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_129_1718_w_1a_083118.csv"),
], "characteristics2018", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_129_1617_w_1a_11212017.csv"),
], "characteristics2017", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_129_1516_w_2a_011717.csv"),
], "characteristics2016", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_129_1415_w_0216161a.txt"),
], "characteristics2015", create_table=True, delimiter="\t")

#### staff file

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_059_1819_l_1a_091019.csv"),
], "staff2019", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_059_1718_l_1a_083118.csv"),
], "staff2018", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_059_1617_l_2a_11212017_csv.csv"),
], "staff2017", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_059_1516_w_2a_011717.csv"),
], "staff2016", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_059_1415_w_0216161a.txt"),
], "staff2015", create_table=True, delimiter="\t")

#### membership file

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_SCH_052_1819_l_1a_091019.csv"),
], "membership2019", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_SCH_052_1718_l_1a_083118.csv"),
], "membership2018", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_SCH_052_1617_l_2a_11212017.csv"),
], "membership2017", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_052_1516_w_2a_011717.csv"),
], "membership2016", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_052_1415_w_0216161a.txt"),
], "membership2015", create_table=True, delimiter="\t")

#### lunch file

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_033_1819_l_1a_091019.csv"),
], "lunch2019", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_033_1718_l_1a_083118.csv"),
], "lunch2018", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_033_1617_l_2a_11212017.csv"),
], "lunch2017", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_033_1516_w_2a_011717.csv"),
], "lunch2016", create_table=True)

load_into_sqlite(conn, [
    os.path.join(input_dir, "ccd_sch_033_1415_w_0216161a.txt"),
], "lunch2015", create_table=True, delimiter="\t")

# #### geocode

load_into_sqlite(conn, [
    os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1819.TXT"),
], "geocodeRaw2019", create_table=True, delimiter="|",
columns=[
    "NCESSCH", "LEAID", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP", "STFIP", "CNTY",
    "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA", "CBSATYPE", "CSA", "NMCSA", "NECTA",
    "NMNECTA", "CD", "SLDL", "SLDU", "SCHOOLYEAR" ])

load_into_sqlite(conn, [
    os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1718/EDGE_GEOCODE_PUBLICSCH_1718.TXT"),
], "geocodeRaw2018", create_table=True, delimiter="|",
columns=[
    "NCESSCH", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP", "STFIP", "CNTY",
    "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA", "CBSATYPE", "CSA", "NMCSA", "NECTA",
    "NMNECTA", "CD", "SLDL", "SLDU", "SCHOOLYEAR" ])

load_into_sqlite(conn, [
    os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1617/EDGE_GEOCODE_PUBLICSCH_1617.TXT"),
], "geocodeRaw2017", create_table=True, delimiter="|",
columns=[
    "NCESSCH", "NAME", "OPSTFIPS", "STREET", "CITY", "STATE", "ZIP", "STFIP", "CNTY",
    "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA", "CBSATYPE", "CSA", "NMCSA", "NECTA",
    "NMNECTA", "CD", "SLDL", "SLDU", "SCHOOLYEAR" ])

load_into_sqlite(conn, [
    os.path.join(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.txt"),
], "geocodeRaw2016", create_table=True, delimiter="\t")

load_into_sqlite(conn, [
    os.path.join(input_dir, "EDGE_GEOIDS_201415_PUBLIC_SCHOOL.csv"),
], "geocodeRaw2015", create_table=True)


conn.close()
