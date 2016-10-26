setwd("~/Dropbox/projects/mid-project/replication")
library(sqldf)
library(plyr)
library(DataCombine)
library(foreign)

Part <- read.csv("gml-MIDB_4.01.csv")
Disp <- read.csv("gml-MIDA_4.01.csv")
COWNDY <- read.dta("ndy-1816-2008.dta")

Part <- adply(Part, 1, summarise, year = seq(styear, endyear)
              )[c("dispnum3", "ccode", "stabb", "year", "sidea", 
                  "revstate", "revtype1", "revtype2", "fatality", "fatalpre", 
                  "hiact","hostlev","orig")]

Part <- with(Part, Part[order(dispnum3, ccode, year),])

NDY <- sqldf("select A.dispnum3 dispnum3, A.ccode ccode1, B.ccode ccode2, 
      A.year year1, B.year year2, A.sidea sidea1, B.sidea sidea2, 
      A.revstate revstate1, B.revstate revstate2, 
      A.revtype1 revtype11, B.revtype1 revtype12, 
      A.revtype2 revtype21, B.revtype2 revtype22, 
      A.fatality fatality1, B.fatality fatality2, 
      A.fatalpre fatalpre1, B.fatalpre fatalpre2, 
      A.hiact hiact1, B.hiact hiact2, A.hostlev hostlev1, B.hostlev hostlev2, 
      A.orig orig1, B.orig orig2
      from Part A join Part B using (dispnum3) 
      where A.ccode != B.ccode AND A.ccode < B.ccode AND 
      sidea1 != sidea2 AND year1 == year2
      order by A.ccode, B.ccode")

NDY$year2 <- NULL
NDY <- rename(NDY, c("year1" = "year"))


NDY <- with(NDY, NDY[order(dispnum3),])
# NDY[!duplicated(NDY$dispnum3, NDY$ccode1, NDY$ccode2, NDY$year), ]
NDY <- unique( NDY[ , 1:ncol(NDY) ] )

NDY$midongoing <- 1

NDY$midonset <- with(NDY, ave(year, dispnum3, FUN = function(x)
  as.integer(c(TRUE, tail(x, -1L) != head(x, -1L) + 1L))))

NDY <- MoveFront(NDY, c("dispnum3", "ccode1", "ccode2", "year", 
                        "midonset", "midongoing"), exact = FALSE)

Disps <- with(Disp, data.frame(dispnum3, hiact, hostlev, mindur, maxdur, outcome, settle,
                               fatality, fatalpre, stmon, endmon))

NDY <- join(NDY, Disps, by=c("dispnum3"), type="left")




# Remove duplicates
# --------------------------------------------------
# This will start the arduous task of removing duplicate observations.

# Identify duplicates.
# R's `duplicated` function doesn't quite do what I want it to do.
# For example, France and Italy have three "new" MID onsets in 1860 (MID#0112, MID#0113, MID#0306), but only the last two are declared "duplicates" because MID#0112 comes first in the data set.
# So, here's what we'll do. 1) create a `dup` variable counting "duplicates" forward. Then, 2) create a `duprev' variable counting backward.
# This would solve our France-Italy 1860 problem.

flag_dups <- function(a, b){
  a$dup <- a$duprev <- a$multiplemids <- NULL
  a$dup <- as.numeric(duplicated(a[,b]))
  a$duprev <- as.numeric(duplicated(a[,b], fromLast=TRUE))
  a$multiplemids <- with(a, ifelse(dup == 1 | duprev == 1, 1, 0))
  return(a)
}

NDY <- flag_dups(NDY, 2:4)

# Create a `removeme` variable. Set it to 0.
NDY$removeme <- 0

# First rule
# ------------------------------
# if there are multiple dyad-years, eliminate the ones that aren't new MID onsets.

NDY$removeme <- with(NDY, ifelse(multiplemids == 1 & midonset == 0, 1, removeme))
NDY <- subset(NDY, !(removeme == 1))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Second rule
# ------------------------------
# keep the highest hostlev.

NDY <- ddply(NDY, c("ccode1", "ccode2", "year"), function(NDY) return(NDY[NDY$hostlev==max(NDY$hostlev),]))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Third rule
# ------------------------------
# keep the highest fatality

NDY <- ddply(NDY, c("ccode1", "ccode2", "year"), function(NDY) return(NDY[NDY$fatality==max(NDY$fatality),]))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Fourth rule
# ------------------------------
# Keep the highest mindur

NDY <- ddply(NDY, c("ccode1", "ccode2", "year"), function(NDY) return(NDY[NDY$mindur==max(NDY$mindur),]))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Fifth rule
# ------------------------------
# Keep whichever one came first.

NDY <- ddply(NDY, c("ccode1", "ccode2", "year"), function(NDY) return(NDY[NDY$stmon==min(NDY$stmon),]))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Unbelievably, we still have three MIDs left (MID#2163, MID#3604, and a few MID#0258s).

# Sixth rule
# Keep the highest fatalpre. This will solve the MID#2163-MID#3604 problem. MID#2163 had more fatalities.

NDY <- ddply(NDY, c("ccode1", "ccode2", "year"), function(NDY) return(NDY[NDY$fatalpre==max(NDY$fatalpre),]))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Seventh and final rule
# Get rid of it if dup == 1.

NDY <- subset(NDY, !(dup == 1))

# Rewrite `dup`, `duprev` and `multiplemids`
NDY <- flag_dups(NDY, 2:4)

# Success
NDY$dup <- NDY$duprev <- NDY$multiplemids <- NDY$removeme <- NULL

NDY$dyad <- with(NDY, as.numeric(paste0("1",sprintf("%03d", ccode1), sprintf("%03d", ccode2))))

# Merge into full COWNDY frame.
# --------------------------------------------------
# This goes to just 2008, so we'll need to carry to 2010.
# We could add rows for South Sudan, but it begins in 2011.

COWNDY$dyad <- with(COWNDY, paste0(sprintf("%03d", ccode1), "-", sprintf("%03d", ccode2)))

Alivein2008 <- subset(COWNDY, year == 2008)

EmptyPDF <- data.frame(
        dyad =unique(Alivein2008$dyad),
	 dates = c("2009-2010"))

require(reshape2)
EmptyPDF <- cbind(EmptyPDF, colsplit(EmptyPDF$dates, "-", c("start", "end")))
EmptyPDF <- adply(EmptyPDF, 1, summarise, year = seq(start, end))[c("dyad", "year")]

EmptyPDF <- cbind(EmptyPDF, colsplit(EmptyPDF$dyad, "-", c("ccode1", "ccode2")))

EmptyPDF$dyad <- NULL
COWNDY$dyad <- NULL

COWNDY <- rbind(COWNDY, EmptyPDF)

# Merge, finally.

Data <- join(COWNDY, NDY, by=c("ccode1", "ccode2", "year"), type="left")

sqldf("select ccode1, ccode2, year, dispnum3, midongoing, midonset from Data where ccode1 == 731 AND ccode2 == 732")

write.table(Data,file="gml-ndy.csv",sep=",",row.names=F,na="")
