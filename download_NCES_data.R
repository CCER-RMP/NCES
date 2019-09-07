
library(readxl)

## Write to delimited file; default to tabs
writeToDelimitedFile <- function(filename, df, sep="\t") {
  write.table(
    df,
    file = filename,
    append = FALSE,
    quote = FALSE,
    sep = sep,
    eol = "\n",
    na = "",
    dec = ".",
    row.names = FALSE,
    col.names = TRUE
  )
}

## Write to tab-separated file
writeToTsv <- function(filename, df) {
  writeToDelimitedFile(filename, df)
}


input_dir <- "C:/Users/jchiu/NCES/input"

urls <- c(
	# from 2015 to 2018, there are 5 files in common core, and a separate geocode file

	# 2018 preliminary
	"https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1718_w_0a_03302018_csv.zip"

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

)

for(url in urls) {
	last_slash_pos <- regexpr("/[^/]*$", url) + 1
	base <- substr(url, last_slash_pos, nchar(url))
	path <- paste(input_dir, base, sep="/")
	if(!file.exists(path)) {
		print(paste("Downloading", url))
		download.file(url, path)

		print(paste("Unzipping", path))
		unzip(path, exdir=input_dir, overwrite=FALSE)
	} else {
		print(paste("Skipping", url))
	}
}

print("Converting geocode file for 2016 to TXT...")
geocode2016 <- paste(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.txt", sep="/")
if(!file.exists(geocode2016)) {
	df <- read_excel(paste(input_dir, "EDGE_GEOCODE_PUBLICSCH_1516/EDGE_GEOCODE_PUBLICSCH_1516.xlsx", sep="/"))
	writeToTsv(geocode2016, df)
}
